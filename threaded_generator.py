from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread, Lock


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Buffer items from an iterable in a separate thread (Python > 3.13).

    This class supports two usage patterns:
    1. Direct iteration (`__iter__`):
       The simplest way to use the generator. It handles starting the thread
       and joining it when iteration completes. Only one consumer can iterate
       at a time. Restarting iteration will also restart the underlying iterable.

    2. Shared consumption (`enqueue` + `join`):
       Multiple consumers can call `enqueue()` to pull items from the same
       underlying iterable concurrently. The main thread must eventually call
       `join()` to ensure resources are cleaned up. To restart iteration with
       `enqueue`, `join()` must be called first.

    Calling `__iter__` after `enqueue` is incompatible; `__iter__` attempts to
    acquire the lock and will block until `join()` has been called to release it.

    The underlying iterable is assumed to be not thread-safe, so only one
    worker thread buffers items into the queue.

    Example:
        >>> list(ThreadedGenerator(range(3), maxsize=2))
        [0, 1, 2]

    Args:
        it (Iterable[T]): The iterable to wrap.
        maxsize (int): Maximum items to buffer.
    """

    def __init__(self, it: Iterable[T], maxsize: int = 1):
        self.it = it
        self.queue: Queue[T] = Queue(maxsize=maxsize)
        self.thread: Thread | None = None
        self.exception: Exception | None = None
        self.lock = Lock()

    def __repr__(self) -> str:
        return f"ThreadedGenerator({repr(self.it)})"

    def run(self) -> None:
        try:
            for value in self.it:
                self.queue.put(value)
        except ShutDown:
            pass
        except Exception as e:
            self.exception = e
        finally:
            self.queue.shutdown()

    def start(self, blocking: bool = True) -> None:
        if self.lock.acquire(blocking=blocking):
            try:
                self.queue.is_shutdown = False
                self.exception = None
                self.thread = Thread(target=self.run, name=repr(self.it))
                self.thread.start()
            except Exception:
                self.lock.release()
                raise

    def enqueue(self) -> Generator[T]:
        """
        Return a generator that yields items from the shared queue.

        This enables the "Shared consumption" pattern where multiple consumers
        process items from the same source iterable in parallel. The generator
        thread starts automatically if it isn't running.

        The caller is responsible for eventually calling `join()` to clean up
        resources (thread and queue).

        Returns:
            Generator[T]: A generator yielding items from the queue.
        """
        self.start(blocking=False)
        return self.iter_queue()

    def iter_queue(self) -> Generator[T]:
        try:
            while True:
                yield self.queue.get()
                self.queue.task_done()
        except ShutDown:
            pass

    def __iter__(self) -> Generator[T]:
        """
        Iterate over buffered items ("Direct iteration" pattern).

        This method manages the full lifecycle: it acquires the lock, starts
        the background thread, yields items, and ensures cleanup via `join()`
        when iteration completes or fails.

        It is incompatible with `enqueue`: if the lock is held (by `enqueue`
        or another `__iter__`), this method will block until `join()` is called.
        """
        self.start(blocking=True)
        try:
            yield from self.iter_queue()
        finally:
            self.terminate()

    def terminate(self, immediate: bool = True) -> None:
        self.queue.shutdown(immediate)
        self.join()

    def join(self) -> None:
        """
        Join the thread and queue, and re-raise any exceptions.

        This method must be called when using `enqueue` to ensure resources
        are cleaned up and the lock is released. It is called automatically
        when using `__iter__`.

        Raises:
            RuntimeError: If an exception occurred in the worker thread.
        """
        if self.thread:
            self.thread.join()
            self.thread = None
        self.queue.shutdown()
        self.queue.join()
        try:
            self.lock.release()
        except RuntimeError:
            pass
        if self.exception:
            msg = f"Exception from {self.thread}"
            try:
                raise RuntimeError(msg) from self.exception
            finally:
                self.exception = None
