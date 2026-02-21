import unittest
import sys
import os

# Ensure we can import from src if running locally without installation
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from threaded_generator import ThreadedGenerator


class TestThreadedGenerator(unittest.TestCase):
    def test_basic_iteration(self):
        """Test that it yields all items from source in order."""
        source = range(10)
        gen = ThreadedGenerator(source)
        result = list(gen)
        self.assertEqual(result, list(source))

    def test_empty_source(self):
        """Test that it handles empty source."""
        source = []
        gen = ThreadedGenerator(source)
        result = list(gen)
        self.assertEqual(result, [])

    def test_small_maxsize(self):
        """Test iteration works with a small maxsize (forcing blocking puts)."""
        source = range(100)
        # maxsize=1 forces the producer thread to block frequently
        gen = ThreadedGenerator(source, maxsize=1)
        result = list(gen)
        self.assertEqual(result, list(source))

    def test_restart(self):
        """Test that the generator can be iterated over multiple times."""
        source = range(10)
        gen = ThreadedGenerator(source)

        # First pass
        result1 = list(gen)
        self.assertEqual(result1, list(source))

        # Second pass (restart)
        result2 = list(gen)
        self.assertEqual(result2, list(source))

    def test_exception_propagation(self):
        """Test that exceptions in the source are propagated to the consumer."""

        def error_gen():
            yield 1
            yield 2
            raise ValueError("Test Error")

        gen = ThreadedGenerator(error_gen())

        # We expect a RuntimeError wrapping the original ValueError
        with self.assertRaises(RuntimeError) as cm:
            list(gen)

        self.assertIsInstance(cm.exception.__cause__, ValueError)
        self.assertEqual(str(cm.exception.__cause__), "Test Error")

    def test_pipeline_exception_propagation(self):
        """Test that exceptions propagate through a chain of ThreadedGenerators."""

        def error_gen():
            yield 1
            raise ValueError("Root Error")

        # Create a pipeline: error_gen -> gen1 -> gen2
        gen1 = ThreadedGenerator(error_gen())
        gen2 = ThreadedGenerator(gen1)

        # Iterating gen2 should eventually raise the error from error_gen
        with self.assertRaises(RuntimeError) as cm:
            list(gen2)

        # The exception might be double-wrapped depending on implementation details,
        # but we definitely expect the root cause to be there.
        # gen1 wraps ValueError in RuntimeError.
        # gen2 wraps gen1's RuntimeError in another RuntimeError.

        first_cause = cm.exception.__cause__
        self.assertIsInstance(first_cause, RuntimeError)

        root_cause = first_cause.__cause__
        self.assertIsInstance(root_cause, ValueError)
        self.assertEqual(str(root_cause), "Root Error")

    def test_shared_consumption(self):
        """Test multiple consumers sharing the same generator via enqueue/join."""
        source = range(10)
        gen = ThreadedGenerator(source)

        # Create two consumers sharing the same underlying queue
        it1 = gen.enqueue()
        it2 = gen.enqueue()

        # Consume both together. Since they share the queue, they will split the items.
        # zip(it1, it2) will pull one from it1, one from it2, etc.
        # So we expect (0, 1), (2, 3), (4, 5), (6, 7), (8, 9)
        results = list(zip(it1, it2))

        # Must call join manually when using enqueue
        gen.join()

        expected = [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9)]
        self.assertEqual(results, expected)

    def test_locking(self):
        """Test that the generator locks during iteration."""
        source = range(10)
        gen = ThreadedGenerator(source)

        # Start one consumption via enqueue
        _ = gen.enqueue()

        # Verify that the lock is held
        self.assertTrue(gen.lock.locked())

        # Clean up
        gen.terminate()
        self.assertFalse(gen.lock.locked())

    def test_terminate(self):
        """Test that terminate stops a potentially infinite source."""
        import time

        def infinite_gen():
            i = 0
            while True:
                yield i
                i += 1
                time.sleep(0.01)  # Slow producer

        gen = ThreadedGenerator(infinite_gen(), maxsize=5)
        it = gen.enqueue()

        # Consume a few
        next(it)
        next(it)

        # Terminate
        gen.terminate(immediate=True)

        # The lock should be released
        self.assertFalse(gen.lock.locked())

        # The thread should be gone
        self.assertIsNone(gen.thread)

    def test_early_break(self):
        """Test that breaking out of __iter__ loop cleans up thread and lock."""
        import time

        def infinite_gen():
            i = 0
            while True:
                yield i
                i += 1
                time.sleep(0.01)

        gen = ThreadedGenerator(infinite_gen(), maxsize=5)

        # Iterate and break early
        for i, item in enumerate(gen):
            if i >= 2:
                break

        # The generator's __iter__ finally block calls terminate()
        # which should release the lock and clear the thread.
        self.assertFalse(gen.lock.locked())
        self.assertIsNone(gen.thread)
        self.assertTrue(gen.queue.is_shutdown)
