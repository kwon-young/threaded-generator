from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Wraps an iterable in a separate thread and uses a queue to buffer items.

    This allows the generation of items to proceed concurrently with their
    consumption, which can improve performance if the generator performs
    I/O or CPU-bound work.

    Args:
        it: The iterable to wrap.
        maxsize: The maximum number of items to buffer in the queue.
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
