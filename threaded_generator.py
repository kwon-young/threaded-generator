from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread, Lock


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Buffer items from an iterable in a separate thread (Python > 3.13).

    Iteration starts a worker thread that buffers items into a queue.
    The thread is automatically joined and resources cleaned up when
    iteration finishes or errors.

    Thread safety is ensured via an internal lock.

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

    def start(self) -> None:
        self.queue.is_shutdown = False
        self.exception = None
        self.thread = Thread(target=self.run, name=repr(self.it))
        self.thread.start()

    def enqueue(self) -> Queue[T]:
        """
        Start the generator if needed and return the underlying queue.

        This enables concurrent access to the iterator items via the queue.

        Returns:
            Queue[T]: The queue buffering the iterator items.
        """
        with self.lock:
            if self.thread is None:
                self.start()
        return self.queue

    def __iter__(self) -> Generator[T]:
        """
        Iterate over buffered items.

        Uses a lock to ensure only one thread iterates at a time.
        """
        with self.lock:
            self.start()
            try:
                while True:
                    yield self.queue.get()
            except ShutDown:
                pass
            finally:
                self.join()

    def join(self) -> None:
        """
        Stop the generator, join the thread, and re-raise any exceptions.

        Raises:
            RuntimeError: If an exception occurred in the worker thread.
        """
        self.queue.shutdown(immediate=True)
        if self.thread:
            self.thread.join()
        if self.exception:
            msg = f"Exception from {self.thread}"
            raise RuntimeError(msg) from self.exception
