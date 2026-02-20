from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Wraps an iterable in a separate thread and uses a queue to buffer items.

    Requires Python > 3.13.

    Example:
        >>> def slow_gen():
        ...     import time
        ...     for i in range(3):
        ...         time.sleep(0.1)
        ...         yield i
        >>> for i in ThreadedGenerator(slow_gen(), maxsize=2):
        ...     print(i)
        0
        1
        2

    The worker thread is dispatched when iteration starts.

    Resource cleanup is handled automatically when iteration stops (either
    by exhaustion, error, or explicit break). The queue then is shut down and
    the worker thread is joined.

    Args:
        it (Iterable[T]): The iterable to wrap.
        maxsize (int): The maximum number of items to buffer in the queue.
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
