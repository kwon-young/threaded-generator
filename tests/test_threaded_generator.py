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
