from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread, Lock


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Buffer items from an iterable in a separate thread (Python > 3.13).

    This class supports two modes of consumption:
    1. Direct iteration (`__iter__`): manages the full lifecycle (start/join).
    2. Concurrent queue access (`enqueue`): allows consuming the queue directly.

    These modes are mutually exclusive via a lock, as the underlying iterable
    is assumed to be not thread-safe.

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
        Start the generator if needed and return a generator over the queue.

        This enables concurrent access to the iterator items via the queue.
        Acquires the lock non-blocking; if the generator is already running
        via `__iter__` (locked), this may not start a new thread or might
        piggyback if the lock logic allows (implementation dependent), but
        intended usage implies exclusivity.

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
        Iterate over buffered items.

        Uses a lock to ensure only one thread iterates at a time.
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
        Stop the generator, join the thread, and re-raise any exceptions.

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
