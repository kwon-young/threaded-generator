# ThreadedGenerator

Buffer items from an iterable in a separate thread.

This library provides `ThreadedGenerator`, a utility to wrap an iterable (like a generator or a slow I/O process) in a background thread. It buffers items in a `queue.Queue` so that the consumer and producer can work concurrently.

This is particularly useful when:
*   The producer (the iterable) is slow (e.g., network requests, disk I/O).
*   The consumer is slow.
*   You want to smooth out bursts of processing.

## Installation

```bash
pip install threaded-generator
```

*(Note: Adjust the package name based on your PyPI release)*

## Usage

`ThreadedGenerator` supports two main usage patterns.

### 1. Direct Iteration (Single Consumer)

This is the simplest usage. It automatically manages the background thread lifecycle. When you start iterating, the thread starts; when you finish or stop, the thread is joined.

```python
import time
from threaded_generator import ThreadedGenerator

def slow_producer():
    for i in range(5):
        time.sleep(0.5)  # Simulate work
        yield i

# Wrap the generator
# maxsize controls the buffer size
gen = ThreadedGenerator(slow_producer(), maxsize=3)

# Iterate just like a normal generator
for item in gen:
    print(f"Got {item}")
```

### 2. Shared Consumption (Multiple Consumers)

You can share a single underlying producer among multiple consumers using `enqueue()`. This requires manual lifecycle management using `join()` or `terminate()`.

```python
from threaded_generator import ThreadedGenerator

source = range(10)
gen = ThreadedGenerator(source, maxsize=5)

# Create two iterators sharing the same source
it1 = gen.enqueue()
it2 = gen.enqueue()

# Pull items from both
print(next(it1)) # 0
print(next(it2)) # 1
print(next(it1)) # 2

# Cleanup is mandatory!
gen.join()
```

## Error Handling

Exceptions raised within the source iterable are caught in the background thread and re-raised in the main thread (wrapped in a `RuntimeError`) when `join()` is called or iteration completes.
