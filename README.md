# Threaded Generator

Easily run iterators in background threads or processes to create parallel producer-consumer pipelines.

This library provides utilities to wrap an iterable (like a generator, a slow I/O process, or a CPU-bound task) in a background thread or process. It buffers items in a queue so that the consumer and producer can work concurrently, smoothing out bursts and improving throughput.

Key Features:
*   **`ThreadedGenerator`**: Run an iterator in a background thread (good for I/O bound tasks).
*   **`ProcessGenerator`**: Run an iterator in a background process (good for CPU bound tasks, avoids GIL).
*   **`ParallelGenerator`**: Split the work of an iterator across multiple parallel workers.
*   **`ShutdownQueue`**: A queue wrapper that handles graceful shutdown signaling between producers and consumers.
*   **`Monitor`**: Real-time terminal visualization of queue sizes and throughput.

## Installation

```bash
pip install threaded-generator
```

## Usage

### 1. Basic Threaded Generation (I/O Bound)

Use `ThreadedGenerator` when your source is slow due to I/O (network, disk).

```python
import time
from threaded_generator import ThreadedGenerator

def slow_io_task():
    for i in range(5):
        time.sleep(0.5)  # Simulate network/disk wait
        yield i

# Buffers up to 3 items in a background thread
gen = ThreadedGenerator(slow_io_task(), maxsize=3)

for item in gen:
    print(f"Got {item}")
```

### 2. Process Generation (CPU Bound)

Use `ProcessGenerator` to bypass the GIL for CPU-intensive tasks.

```python
from threaded_generator import ProcessGenerator

def heavy_computation():
    for i in range(5):
        # Simulate heavy CPU work
        yield sum(range(1_000_000 * i))

gen = ProcessGenerator(heavy_computation(), maxsize=2)

for result in gen:
    print(result)
```

### 3. Parallel Processing (Multiple Workers)

Use `ParallelGenerator` with `num_workers > 1` to distribute work.

**Important:** You must decorate the generator function with `@partial_generator`. This allows each worker to create its own fresh instance of the iterator.

```python
from threaded_generator import ParallelGenerator, partial_generator
import time

@partial_generator
def process_data(x):
    # Simulate work
    time.sleep(0.5)
    yield x * x

# Spawns 4 worker processes to process items in parallel
gen = ParallelGenerator(process_data(10), num_workers=4, maxsize=10)

for res in gen:
    print(res)
```

### 4. Monitoring

Visualize the performance of your pipeline in the terminal.

```python
from threaded_generator import ThreadedGenerator, Monitor
import time

monitor = Monitor()

# Pass the monitor to the generator
gen = ThreadedGenerator(range(100), monitor=monitor, name="MyGen")

with monitor:
    for item in gen:
        time.sleep(0.1) # Simulate slow consumption
```

## Advanced Usage

### Shared Consumption

You can share a single underlying producer among multiple consumers using the context manager.

```python
gen = ThreadedGenerator(range(10), maxsize=5)

with gen:
    it1 = iter(gen)
    it2 = iter(gen)

    # Items are distributed between it1 and it2
    print(next(it1))
    print(next(it2))
```

### ShutdownQueue

Use `ShutdownQueue` directly if you need a queue that supports graceful shutdown signaling.

```python
import multiprocessing as mp
from queue import ShutDown
from threaded_generator import ShutdownQueue

# Create a shutdown-capable queue backed by a multiprocessing Queue
sq = ShutdownQueue(maxsize=10, queue_type=mp.Queue)

# Producer
sq.put(1)
sq.put(2)
sq.shutdown()  # Signal end of stream

# Consumer
try:
    while True:
        print(sq.get())
except ShutDown:
    print("Queue shut down")
```

### Error Handling

Exceptions raised within the source iterable are caught in the background worker and re-raised in the main thread (wrapped in a `RuntimeError`) when `join()` is called or iteration completes.

## Credits

The original idea for `ThreadedGenerator` (combining a generator, a thread, and a queue) is attributed to [everilae](https://github.com/everilae) and their [GitHub Gist](https://gist.github.com/everilae/9697228).
