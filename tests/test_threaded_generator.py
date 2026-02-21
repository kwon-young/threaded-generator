"""
High-level Test Plan for ThreadedGenerator

1. Basic Iteration:
   - Ensure it yields all items from source.
   - Ensure it handles empty source.
   - Verify ordering is preserved.

2. Exception Propagation:
   - If the source iterable raises an exception, the consumer (iterating over ThreadedGenerator)
     should eventually receive that exception (wrapped or direct).

3. Reusability:
   - Ensure the generator can be iterated over multiple times (since __iter__ restarts the thread).

4. Concurrency/Queue Sizing:
   - Verify `maxsize` parameter is respected (producer blocks when queue is full).

5. Explicit Termination:
   - Test `terminate()` and `join()` behavior.

6. Locking behavior:
   - Verify that simultaneous iterations block or are serialized correctly.
"""

import unittest
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
                time.sleep(0.01) # Slow producer

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

    def test_locking(self):
        """Test that the generator locks during iteration."""
        source = range(10)
        gen = ThreadedGenerator(source)

        # Start one consumption via enqueue
        it = gen.enqueue()
        
        # Try to start another consumption immediately via enqueue.
        # Since enqueue uses start(blocking=False), the internal lock check inside start
        # might succeed if it just checks if it's already running? 
        # No, start() acquires the lock. 
        # If the lock is held, start(blocking=False) returns None? 
        # Let's check the code:
        # if self.lock.acquire(blocking=blocking): ...
        
        # So if we call gen.start(blocking=False) manually, it should return None/False (impl detail)
        # But enqueue calls start(blocking=False) and ignores return value.
        # Wait, if start(blocking=False) fails to acquire lock, it skips starting the thread.
        # But it returns the iterator from iter_queue() anyway.
        # This allows multiple enqueues to share the SAME thread/queue (which is the feature tested in test_shared_consumption).
        
        # However, __iter__ uses blocking=True.
        # So if we have an active enqueue, __iter__ should block.
        # Testing blocking behavior is tricky. 
        
        # Instead, let's test that we cannot misuse it by trying to run two SEPARATE threads on the same generator 
        # if the first one hasn't finished. 
        # Actually, the class design allows multiple consumers on ONE producer thread.
        
        # Let's verify that the lock is indeed held.
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
                time.sleep(0.01) # Slow producer

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
