"""
This module provides a queue wrapper that supports graceful shutdown signaling.

It allows producers to signal that no more items will be produced, and consumers
to detect this signal and stop iteration. It handles both immediate shutdowns
(aborting operations) and graceful shutdowns (processing remaining items).

Key components:

- :class:`ShutdownQueue`: A wrapper around standard queues (like multiprocessing.Queue)
  that adds shutdown semantics.

Examples
--------
.. code-block:: python

    >>> import multiprocessing as mp
    >>> from queue import Empty, ShutDown
    >>>
    >>> # Create a shutdown-capable queue
    >>> sq = ShutdownQueue(maxsize=10, queue_type=mp.Queue)
    >>>
    >>> # Producer
    >>> sq.put(1)
    >>> sq.put(2)
    >>> sq.shutdown()  # Signal end of stream
    >>>
    >>> # Consumer
    >>> try:
    ...     while True:
    ...         print(sq.get())
    ... except ShutDown:
    ...     print("Queue shut down")
    1
    2
    Queue shut down
"""

from typing import (
    Type,
    Generator,
    Protocol,
    TypeGuard,
)
import multiprocessing as mp
from queue import Full, Empty, ShutDown
import time


class Q[T](Protocol):
    """Protocol for a standard Queue interface."""

    def __init__(self, maxsize: int = 0) -> None: ...

    def put(
        self, item: T, block: bool = True, timeout: float | None = None
    ) -> None: ...

    def get(self, block: bool = True, timeout: float | None = None) -> T: ...

    def qsize(self) -> int: ...


class SQ[T](Q[T], Protocol):
    """Protocol for a Queue that supports shutdown operations."""

    def shutdown(self, immediate: bool = False) -> None: ...

    is_shutdown: bool


class Sentinel:
    pass


def is_not_sentinel[T](x: T | Type[Sentinel]) -> TypeGuard[T]:
    return x is not Sentinel


class ShutdownQueue[T]:
    """
    A wrapper class that adds shutdown capabilities to a standard queue.

    This class wraps a queue implementation (like :class:`multiprocessing.Queue`)
    and adds mechanisms to signal shutdown. When shut down, consumers reading
    from the queue will eventually receive a :class:`queue.ShutDown` exception.

    :param maxsize: Maximum size of the queue. Defaults to 0 (infinite).
    :type maxsize: int
    :param queue_type: The class of the underlying queue to use. Defaults to :class:`multiprocessing.Queue`.
    :type queue_type: Type[Q] | None
    :param poll_time: Interval in seconds to check for shutdown signals while blocking.
    :type poll_time: float
    """

    def __init__(
        self,
        maxsize: int = 0,
        queue_type: Type[Q] | None = None,
        poll_time: float = 1,
    ):
        if queue_type is None:
            queue_type = mp.Queue
        self.queue: Q[T | Type[Sentinel]] = queue_type(maxsize)
        self.shutdown_event = mp.Event()
        self.immediate = mp.Value("b", False)
        self.poll_time = poll_time

    def timeout(
        self,
        t: Empty | Full,
        block: bool = True,
        timeout: float | None = None,
    ) -> Generator[float]:
        t0 = time.monotonic()
        if timeout is None:
            timeout = float("inf")
        time_limit = t0 + timeout
        remaining = timeout
        while True:
            poll_time = min(self.poll_time, remaining)
            yield poll_time
            t1 = time.monotonic()
            if block is False or t1 > time_limit:
                raise t
            remaining = time_limit - time.monotonic()

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        """
        Remove and return an item from the queue.

        If the queue is empty and shut down, raises :class:`queue.ShutDown`.

        :param block: If True, block until an item is available.
        :param timeout: Time to wait if blocking.
        :return: The item retrieved from the queue.
        :raises ShutDown: If the queue has been shut down.
        :raises Empty: If the queue is empty and block is False or timeout reached (before shutdown).
        """
        for poll_time in self.timeout(Empty(), block, timeout):
            with self.immediate.get_lock():
                if self.shutdown_event.is_set() and self.immediate.value:
                    raise ShutDown()
            try:
                x = self.queue.get(block, poll_time)
                break
            except Empty:
                # mp.Queue.empty is not reliable, as per the doc
                if self.shutdown_event.is_set():
                    raise ShutDown()
        if is_not_sentinel(x):
            return x
        try:
            self.queue.put(Sentinel, False)
        except Full:
            pass
        raise ShutDown()

    def put(
        self, obj: T, block: bool = True, timeout: float | None = None
    ) -> None:
        """
        Put an item into the queue.

        :param obj: The item to put.
        :param block: If True, block if the queue is full.
        :param timeout: Time to wait if blocking.
        :raises ShutDown: If the queue has been shut down.
        :raises Full: If the queue is full and block is False or timeout reached.
        """
        for poll_time in self.timeout(Full(), block, timeout):
            if self.shutdown_event.is_set():
                raise ShutDown()
            try:
                self.queue.put(obj, block, poll_time)
                break
            except Full:
                pass

    def shutdown(self, immediate: bool = False) -> None:
        """
        Signal the queue to shut down.
        Can block up to self.poll_time.

        :param immediate: If True, consumers raise ShutDown immediately upon next access
                          without draining remaining items. If False, consumers drain
                          the queue until the Sentinel is reached.
        """
        self.shutdown_event.set()
        with self.immediate.get_lock():
            self.immediate.value = immediate
            self.drain()
        try:
            self.queue.put(Sentinel, True, self.poll_time)
        except Full:
            # for very slow consumers, let the poll mechanism work
            pass

    @property
    def is_shutdown(self) -> bool:
        """
        Check or set the shutdown signal.

        Getter:
            Returns True if the shutdown signal has been set.

        Setter:
            Manually set or clear the shutdown state.
            Clearing shutdown state (False) also drains the queue of existing items to reset it.
            Setting it to True triggers a shutdown.
        """
        return self.shutdown_event.is_set()

    @is_shutdown.setter
    def is_shutdown(self, state) -> None:
        if state is False:
            with self.immediate.get_lock():
                self.shutdown_event.clear()
                self.immediate.value = False
                self.drain()
        else:
            self.shutdown()

    def drain(self) -> None:
        while True:
            try:
                self.queue.get(False)
            except Empty:
                break

    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self.queue.qsize()
