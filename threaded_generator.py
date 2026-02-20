from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Buffer items from an iterable in a separate thread (Python > 3.13).

    The worker thread starts on iteration and cleans up automatically (joining
    the thread and shutting down the queue) when iteration stops or errors.

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
        self.exception: Exception | None = None

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

    def __iter__(self) -> Generator[T]:
        self.queue.is_shutdown = False
        self.exception = None
        thread = Thread(target=self.run, name=repr(self.it))
        thread.start()
        try:
            while True:
                yield self.queue.get()
        except ShutDown:
            pass
        finally:
            self.queue.shutdown(immediate=True)
            thread.join()
            if self.exception:
                msg = f"Exception from {thread}"
                raise RuntimeError(msg) from self.exception
