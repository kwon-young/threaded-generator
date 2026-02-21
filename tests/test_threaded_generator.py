import unittest
from threaded_generator import ThreadedGenerator, ParallelGenerator


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

        # gen1 wraps ValueError in RuntimeError.
        # gen2 wraps gen1's RuntimeError in another RuntimeError.

        first_cause = cm.exception.__cause__
        self.assertIsInstance(first_cause, RuntimeError)

        root_cause = first_cause.__cause__
        self.assertIsInstance(root_cause, ValueError)
        self.assertEqual(str(root_cause), "Root Error")

    def test_shared_consumption(self):
        """Test multiple consumers sharing the same generator via context manager."""
        source = range(10)
        gen = ParallelGenerator(source, num_workers=1)

        with gen:
            # zip(gen, gen) will create two iterators on the same underlying queue.
            # They should consume the source cooperatively.
            results = [x for pair in zip(gen, gen) for x in pair]

        expected = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        self.assertEqual(sorted(results), expected)

    def test_context_manager_lifecycle(self):
        """Test that context manager handles start/join."""
        source = range(10)
        gen = ThreadedGenerator(source)

        # Lock should not be held initially
        self.assertFalse(gen.lock.locked())
        self.assertEqual(len(gen.workers), 0)

        with gen:
            # Should have started workers
            self.assertEqual(len(gen.workers), 1)
            self.assertTrue(gen.workers[0].is_alive())

            # Consume
            next(iter(gen))

        # Should be cleaned up
        self.assertFalse(gen.workers[0].is_alive())
        self.assertTrue(gen.queue.is_shutdown)
        self.assertEqual(gen.refcount, 0)

    def test_early_break(self):
        """Test that breaking out of loop cleans up."""
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

        # Checks internal state to ensure workers are stopped
        for worker in gen.workers:
            self.assertFalse(worker.is_alive())
        self.assertTrue(gen.queue.is_shutdown)
        self.assertEqual(gen.refcount, 0)
