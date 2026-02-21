"""
This module provides generator wrappers that execute source iterators in separate threads
or processes, allowing for parallel producer-consumer pipelines.

Key components:

- :class:`ThreadedGenerator`: Runs the source iterator in a separate thread.
- :class:`ProcessGenerator`: Runs the source iterator in a separate process.
- :class:`ParallelGenerator`: Base class supporting multiple workers (threads or processes).

Examples
--------
.. code-block:: python

    >>> import time
    >>> def slow_source():
    ...     for i in range(5):
    ...         time.sleep(0.1)
    ...         yield i
    ...
    >>> # Wrap the slow source in a threaded generator
    >>> gen = ThreadedGenerator(slow_source(), maxsize=3)
    >>> for item in gen:
    ...     print(item)  # Consumption happens in main thread, production in background
    0
    1
    2
    3
    4

    >>> # Using ProcessGenerator for CPU-bound tasks
    >>> def cpu_bound_source():
    ...     for i in range(5):
    ...         yield sum(range(1000000 * i))
    ...
    >>> gen = ProcessGenerator(cpu_bound_source(), maxsize=2)
    >>> list(gen)
    [0, 0, 1999999000000, ...]

    >>> # Using ParallelGenerator with multiple workers
    >>> # Note: You must wrap the generator function with @partial_generator.
    >>> # This is required because standard generators cannot be pickled or restarted.
    >>> # The wrapper delays execution so each worker process can create its own
    >>> # fresh iterator instance.
    >>> @partial_generator
    ... def parallel_source(x):
    ...     yield x * x
    ...
    >>> # This will spawn 4 worker processes
    >>> gen = ParallelGenerator(parallel_source(2), num_workers=4)
    >>> list(gen)
    [4, 4, 4, 4]
"""

from typing import (
    Iterable,
    Type,
    Generator,
    Literal,
    Iterator,
    Callable,
    cast,
)
from functools import wraps, partial
import multiprocessing as mp
import multiprocessing.synchronize as ms
import multiprocessing.queues as mq
from multiprocessing.sharedctypes import Synchronized
from queue import Queue, ShutDown, Empty
import threading as th
import time
import os
from .shutdown_queue import Q, SQ, ShutdownQueue
from .monitor import Monitor


class PartialGenerator[T]:
    """
    A wrapper class that delays the execution of a generator function.

    :param f: A callable that takes no arguments and returns an Iterator[T].
              This is typically a functools.partial object.
    :type f: Callable[[], Iterator[T]]
    """

    def __init__(self, f: Callable[[], Iterator[T]]):
        self.f: Callable[[], Iterator[T]] = f

    def __iter__(self) -> Iterator[T]:
        """
        Execute the stored callable to create and return the iterator.

        :return: The iterator resulting from calling the stored function.
        :rtype: Iterator[T]
        """
        return iter(self.f())


def partial_generator[T, **P](
    f: Callable[P, Iterator[T]],
) -> Callable[P, PartialGenerator[T]]:
    """
    A decorator that transforms a generator function into one that returns a PartialGenerator.
    Decorate all generators that are passed to ParallelGenerator with this.

    :param f: The generator function to wrap.
    :type f: Callable[P, Iterator[T]]
    :return: A wrapper function that, when called, returns a PartialGenerator
             instance instead of immediately executing the generator logic.
    :rtype: Callable[P, PartialGenerator[T]]
    """

    @wraps(f)
    def gen(*args: P.args, **kwargs: P.kwargs) -> PartialGenerator[T]:
        return PartialGenerator(partial(f, *args, **kwargs))

    return gen


Task = mp.Process | th.Thread
Method = Literal["process", "thread"]
method_task: dict[Method, Type[Task]] = {
    "process": mp.Process,
    "thread": th.Thread,
}
default_method_queue: dict[Method, Type[Q]] = {
    "process": mp.Queue,
    "thread": Queue,
}


def method_queue[T](
    method: Method, queue_type: Type[Q[T]] | None = None
) -> Type[SQ[T]]:
    if queue_type is None:
        queue_type = default_method_queue[method]
    shutdown_queue_type: Type[SQ[T]]
    if hasattr(queue_type, "shutdown"):
        shutdown_queue_type = cast(Type[SQ[T]], queue_type)
    else:
        shutdown_queue_type = cast(
            Type[ShutdownQueue[T]],
            partial(ShutdownQueue, queue_type=queue_type),
        )
    return shutdown_queue_type


@partial_generator
def iter_queue[T](queue: SQ[T]) -> Generator[T]:
    """
    Generator that yields items from a ShutdownQueue until it is shut down.

    This serves as the consumer side of the pipeline. It continuously pulls items
    from the queue. When the producer worker finishes and the queue is shut down
    (raising a ShutDown exception internally or receiving a Sentinel), this
    generator catches the signal and stops yielding, effectively closing the stream.

    :param queue: The queue to consume items from.
    :type queue: SQ[T]
    :yield: Items retrieved from the queue.
    :rtype: Generator[T, None, None]
    """
    try:
        while True:
            x = queue.get()
            yield x
    except ShutDown:
        pass
    finally:
        queue.shutdown(immediate=True)


def iterable_worker[T](
    it: Iterable[T],
    queue: SQ[T],
    barrier: ms.Barrier,
    name: str,
    worker_id: int,
    error_queue: Q[tuple[str, Exception]],
) -> None:
    try:
        for x in it:
            try:
                queue.put(x)
            except ShutDown:
                break
    except Exception as e:
        error_queue.put((name, e))
        queue.shutdown(True)
    finally:
        barrier.wait()
        queue.shutdown()


class ParallelGenerator[T]:
    """
    A generator wrapper that executes the source iterator in separate workers (threads or processes).

    :param it: The source iterable to consume.
    :type it: Iterable[T]
    :param num_workers: Number of worker processes/threads to spawn. Defaults to 1.
    :type num_workers: int
    :param maxsize: Maximum size of the internal queue. Defaults to 1.
    :type maxsize: int
    :param disable: If True, run synchronously in the main thread/process without workers.
    :type disable: bool
    :param name: Name for the generator, used in monitoring/logging.
    :type name: str | None
    :param method: Execution method, either "process" or "thread".
    :type method: Method
    :param queue_type: Custom queue class to use. Defaults to :class:`multiprocessing.Queue` if method is "process",
                       or :class:`queue.Queue` if method is "thread".
    :type queue_type: Type[Q[T]] | None
    :param monitor: Monitor instance for tracking performance statistics.
    :type monitor: Monitor | None

    .. warning::
       When using multiple workers (`num_workers > 1`), the input iterable `it`
       **must** be a factory or a function wrapped with :func:`partial_generator`.
       Standard python generators cannot be pickled or restarted, so each worker
       needs a fresh iterator instance.
    """

    def __init__(
        self,
        it: Iterable[T],
        num_workers: int = 1,
        maxsize: int = 1,
        disable: bool = False,
        name: str | None = None,
        method: Method = "process",
        queue_type: Type[Q[T]] | None = None,
        monitor: Monitor | None = None,
    ):
        self.it: Iterable[T] = it
        self.num_workers: int = 0 if disable else num_workers
        self.refcount: Synchronized[int]
        self.error_queue: mq.Queue[tuple[str, Exception]] = mp.Queue()
        self.maxsize = maxsize
        self.refcount = mp.Value("i", 0)
        self.queue = method_queue(method, queue_type)(maxsize=maxsize)
        self.method = method
        self.workers: list[Task] = []
        self.name: str = name or f"{type(self).__name__}({it})"
        self.monitor = monitor

    def __enter__(self) -> "ParallelGenerator[T]":
        """
        Start the worker processes/threads and prepare the generator.

        :return: The instance itself.
        :rtype: ParallelGenerator[T]
        """
        with self.refcount.get_lock():
            self.refcount.value += 1
            if self.refcount.value == 1:
                if self.num_workers > 0:
                    barrier = mp.Barrier(self.num_workers)
                self.queue.is_shutdown = False
                self.workers = []
                for i in range(self.num_workers):
                    name = f"{self.name}-{i}"
                    worker = method_task[self.method](
                        target=iterable_worker,
                        args=(
                            self.iter_stat(self.it, "p"),
                            self.queue,
                            barrier,
                            name,
                            i,
                            self.error_queue,
                        ),
                        name=name,
                    )
                    worker.start()
                    self.workers.append(worker)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Clean up resources, joining or terminating workers.
        """
        if exc_type is not None:
            self.terminate()
        else:
            self.join()

    def join(self) -> None:
        """
        Wait for all workers to finish and check for errors propagated from workers.

        :raises RuntimeError: If an exception occurred in any worker.
        """
        for worker in self.workers:
            worker.join()
        with self.refcount.get_lock():
            self.refcount.value -= 1
            try:
                name, e = self.error_queue.get(False)
                msg = f"Exception from {name}"
                raise RuntimeError(msg) from e
            except Empty:
                pass

    def terminate(self) -> None:
        """
        Signal the queue to shutdown immediately and wait for workers to join.
        """
        self.queue.shutdown(True)
        self.join()

    @partial_generator
    def iter_stat(self, it: Iterable[T], t: Literal["p", "c"]) -> Generator[T]:
        if self.monitor:
            t0 = time.monotonic()
            tid = (os.getpid(), th.get_ident())
            for x in it:
                yield x
                t1 = time.monotonic()
                delta = t1 - t0
                self.monitor.put(
                    (self.name, t, delta, tid, self.queue.qsize(), self.maxsize)
                )
                t0 = time.monotonic()
        else:
            yield from it

    def __iter__(self) -> Generator[T]:
        """
        Iterate over the items produced by the generator.

        :yield: Items from the queue (or directly from source if workers disabled).
        :rtype: Generator[T, None, None]
        """
        if self.num_workers == 0:
            yield from self.iter_stat(self.it, "c")
        else:
            with self:
                yield from self.iter_stat(iter_queue(self.queue), "c")


class ThreadedGenerator[T](ParallelGenerator[T]):
    """
    A generator that runs the source iterator in a separate thread.

    Communication is done using a :class:`queue.Queue` by default.
    If this is chained with a process based Generator, you need to use
    a :class:`multiprocessing.Queue` for the queue_type.

    :param it: The source iterable.
    :type it: Iterable[T]
    :param maxsize: Max queue size.
    :type maxsize: int
    :param disable: If True, run in main thread.
    :type disable: bool
    :param name: Name for monitoring.
    :type name: str | None
    :param queue_type: Custom queue type.
    :type queue_type: Type[Q[T]] | None
    :param monitor: Monitor instance.
    :type monitor: Monitor | None

    Example
    -------
    .. code-block:: python

        >>> gen = ThreadedGenerator(range(10), maxsize=5)
        >>> list(gen)
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    """

    def __init__(
        self,
        it: Iterable[T],
        maxsize: int = 1,
        disable: bool = False,
        name: str | None = None,
        queue_type: Type[Q[T]] | None = None,
        monitor: Monitor | None = None,
    ):
        super().__init__(
            it, 1, maxsize, disable, name, "thread", queue_type, monitor
        )


class ProcessGenerator[T](ParallelGenerator[T]):
    """
    A generator that runs the source iterator in a separate process.

    Communication is done using a :class:`multiprocessing.Queue` by default.

    :param it: The source iterable.
    :type it: Iterable[T]
    :param maxsize: Max queue size.
    :type maxsize: int
    :param disable: If True, run in main process.
    :type disable: bool
    :param name: Name for monitoring.
    :type name: str | None
    :param queue_type: Custom queue type.
    :type queue_type: Type[Q[T]] | None
    :param monitor: Monitor instance.
    :type monitor: Monitor | None

    Example
    -------
    .. code-block:: python

        >>> gen = ProcessGenerator(range(10), maxsize=5)
        >>> list(gen)
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    """

    def __init__(
        self,
        it: Iterable[T],
        maxsize: int = 1,
        disable: bool = False,
        name: str | None = None,
        queue_type: Type[Q[T]] | None = None,
        monitor: Monitor | None = None,
    ):
        super().__init__(
            it, 1, maxsize, disable, name, "process", queue_type, monitor
        )
