from typing import Generator, Generic, TypeVar, Iterable
from queue import Queue, ShutDown
from threading import Thread, Lock


T = TypeVar("T")


class ThreadedGenerator(Generic[T]):
    """
    Buffer items from an iterable in a separate thread (Python > 3.13).

    Supports two usage patterns:
    1. Direct iteration (`__iter__`): Single consumer, auto-manages thread/lifecycle.
       Restarts underlying iterable on new iteration. Blocks if lock held.
    2. Shared consumption (`enqueue` + `join`): Concurrent consumers share one
       iterable. Requires manual `join()` to cleanup/release lock before restart.

    `__iter__` blocks if `enqueue` holds the lock (wait for `join()`).
    Assumes underlying iterable is not thread-safe (single producer).

    Example:
        >>> list(ThreadedGenerator(range(3), maxsize=2))
        [0, 1, 2]

    Args:
        it (Iterable[T]): The iterable to wrap.
        maxsize (int): Maximum items to buffer.
    """

    def __init__(self, it: Iterable[T], maxsize: int = 1,
                 disable: bool = False):
        self.it = it
        self.queue: Queue[T] = Queue(maxsize=maxsize)
        self.thread: Thread | None = None
        self.exception: Exception | None = None
        self.lock = Lock()
        self.disable = disable

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
        Return a generator yielding from the shared queue.
        Starts thread if needed. Caller must call `join()` to cleanup.

        Returns:
            Generator[T]: A generator yielding items from the queue.
        """
        self.start(blocking=False)
        return self.iter_queue()

    def iter_queue(self) -> Generator[T]:
        try:
            while True:
                x = self.queue.get()
                self.queue.task_done()
                yield x
        except ShutDown:
            pass

    def __iter__(self) -> Generator[T]:
        """
        Iterate over buffered items. Manages lifecycle (start/join).
        Blocks if lock held (by `enqueue`/`__iter__`) until `join()` is called.

        Returns:
            Generator[T]: A generator yielding items from the queue.
        """
        if self.disable:
            yield from self.it
        else:
            self.start(blocking=True)
            try:
                yield from self.iter_queue()
            finally:
                self.terminate()

    def terminate(self, immediate: bool = True) -> None:
        """
        Signal queue shutdown and join thread.

        Args:
            immediate (bool): If True, remaining consumers will be stopped
                              immediately.
        """
        self.queue.shutdown(immediate)
        self.join()

    def join(self) -> None:
        """
        Join thread/queue, release lock, and re-raise exceptions.
        Mandatory for `enqueue` cleanup; automatic for `__iter__`.

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
