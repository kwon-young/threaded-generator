from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread, Lock


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Buffer items from an iterable in a separate thread (Python > 3.13).

    The worker thread starts on iteration and cleans up automatically (joining
    the thread and shutting down the queue) when iteration stops or errors.

    Note:
        This class is NOT thread-safe. Like standard Python generators,
        the same instance should not be iterated by multiple threads simultaneously.

    Example:
        >>> list(ThreadedGenerator(range(3), maxsize=2))
        [0, 1, 2]

    Args:
        it (Iterable[T]): The iterable to wrap.
        maxsize (int): Max items to buffer.
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
        with self.lock:
            if self.thread is None:
                self.start()
        return self.queue

    def __iter__(self) -> Generator[T]:
        """
        Iterate over the buffered items.

        Note:
            This method is not thread-safe. Do not call this on the same instance
            from multiple threads.
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
        self.queue.shutdown(immediate=True)
        if self.thread:
            self.thread.join()
        if self.exception:
            msg = f"Exception from {self.thread}"
            raise RuntimeError(msg) from self.exception
