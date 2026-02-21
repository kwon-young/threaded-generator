import time
import math
import multiprocessing
import numpy as np
from typing import Iterator, Iterable, List
from threaded_generator import ThreadedGenerator, Monitor

# Configuration
ITEMS = 50
ITEMS_PER_WORKER = 20
WORKERS = 4
IO_DELAY = 0.05  # Simulate network/disk latency


def fast_io_source(count: int = ITEMS) -> Iterator[int]:
    """Simulates a source that has some IO latency (e.g. reading files)."""
    for i in range(count):
        time.sleep(IO_DELAY)
        yield i


def slow_cpu_process(iterable: Iterable[int]) -> Iterator[int]:
    """Simulates a CPU-intensive transformation."""
    for item in iterable:
        # Busy work
        matrix = np.random.random((500, 500))
        _ = np.linalg.svd(matrix)
        yield item * 2


def fast_io_sink(iterable: Iterable[int]) -> List[int]:
    """Simulates a sink that has some IO latency (e.g. writing files)."""
    results = []
    for item in iterable:
        time.sleep(IO_DELAY)
        results.append(item)
    return results


def run_pipeline(use_threads: bool) -> float:
    name = "ThreadedGenerator" if use_threads else "Standard Generator"
    print(f"Running {name} Pipeline (Single Process)...")
    start = time.time()

    # If use_threads=False, ThreadedGenerator acts as a simple pass-through wrapper
    # thanks to disable=True.
    disable = not use_threads

    # We only want to monitor if threads are actually enabled
    with Monitor() as monitor:
        # 1. Source (IO)
        source = fast_io_source(ITEMS)
        source_gen = ThreadedGenerator(
            source,
            maxsize=0,
            disable=disable,
            monitor=monitor,
            name="SourceGen",
        )

        # 2. Process (CPU)
        processed = slow_cpu_process(source_gen)

        # 3. Sink (IO)
        # We wrap the processed stream so the CPU doesn't block waiting for the Sink
        processed_gen = ThreadedGenerator(
            processed,
            maxsize=5,
            disable=disable,
            monitor=monitor,
            name="ProcessedGen",
        )

        _ = fast_io_sink(processed_gen)

    duration = time.time() - start
    print(f"{name} Duration: {duration:.2f}s")
    return duration


def run_worker_pipeline(use_threads: bool) -> float:
    """
    Function to be run in a subprocess.
    Runs the full pipeline (Source -> CPU -> Sink) for a subset of items.
    """
    start = time.time()

    disable = not use_threads

    # 1. Source (IO)
    source = fast_io_source(ITEMS_PER_WORKER)
    source_gen = ThreadedGenerator(
        source, maxsize=3, disable=disable, name="WorkerSourceGen"
    )

    # 2. Process (CPU)
    processed = slow_cpu_process(source_gen)

    # 3. Sink (IO)
    processed_gen = ThreadedGenerator(
        processed, maxsize=3, disable=disable, name="WorkerProcessedGen"
    )

    _ = fast_io_sink(processed_gen)

    return time.time() - start


def run_mp_benchmark(use_threads: bool) -> float:
    name = "Threaded" if use_threads else "Standard"
    print(f"Running {name} Pipeline with {WORKERS} subprocesses...")

    start_total = time.time()

    # Create a pool of workers
    with multiprocessing.Pool(processes=WORKERS) as pool:
        # map blocks until all are done
        durations = pool.map(run_worker_pipeline, [use_threads] * WORKERS)

    total_duration = time.time() - start_total
    avg_worker_duration = sum(durations) / len(durations)

    print(f"{name} Total Duration: {total_duration:.2f}s")
    print(f"{name} Avg Worker Duration: {avg_worker_duration:.2f}s")
    return total_duration


if __name__ == "__main__":
    # print("=== Single Process Benchmark ===")
    # print(f"Processing {ITEMS} items.")
    # print(f"Per item: IO Source {IO_DELAY}s, CPU Work, IO Sink {IO_DELAY}s\n")

    # t_std = run_pipeline(use_threads=False)
    # print("-" * 20)
    t_thread = run_pipeline(use_threads=True)

    # print("-" * 20)
    # improvement = (t_std - t_thread) / t_std * 100
    # print(f"Improvement: {improvement:.1f}%")
    #
    # print("\n=== Multiprocessing Benchmark ===")
    # print(f"Items per worker: {ITEMS_PER_WORKER}, Workers: {WORKERS}")
    # print(f"Per item: IO {IO_DELAY}s, Heavy CPU, IO {IO_DELAY}s\n")
    #
    # t_std_mp = run_mp_benchmark(use_threads=False)
    # print("-" * 20)
    # t_thread_mp = run_mp_benchmark(use_threads=True)
    #
    # print("-" * 20)
    # improvement_mp = (t_std_mp - t_thread_mp) / t_std_mp * 100
    # print(f"Improvement: {improvement_mp:.1f}%")
