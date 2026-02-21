from multiprocessing import Process, Event
import multiprocessing.synchronize as ms
from multiprocessing.managers import BaseManager
from queue import Queue
import time
import socket
import pickle
from typing import TYPE_CHECKING
import random
import atexit


Address = tuple[str, int]


if TYPE_CHECKING:

    class AddressQueueManager(BaseManager):
        def get_queue(self) -> Queue[Address]: ...
else:

    class AddressQueueManager(BaseManager):
        pass


AddressQueueManager.register("get_queue")


def serve_address_queue(
    port: int,
    authkey: bytes,
    ready: ms.Event,
    terminate: ms.Event,
    maxsize: int = 0,
) -> None:
    queue: Queue[Address] = Queue(maxsize)
    AddressQueueManager.register("get_queue", callable=lambda: queue)
    with AddressQueueManager(address=("", port), authkey=authkey):
        ready.set()
        terminate.wait()


class Timeout:
    def __init__(self, timeout: float | None):
        self.timeout = timeout
        self.start = time.monotonic()

    def remaining(self) -> float | None:
        if self.timeout is None:
            return None
        rem = self.timeout - (time.monotonic() - self.start)
        if rem < 0:
            raise TimeoutError
        return rem


class DistributedQueue[T]:
    def __init__(
        self,
        address: Address,
        authkey: bytes = b"abracadabra",
        master: bool = False,
        maxsize: int = 0,
        ip: str = "localhost",
    ) -> None:
        self.ip = ip
        self.master = None
        if master:
            ready = Event()
            self._terminate = Event()
            self.master = Process(
                target=serve_address_queue,
                args=(address[1], authkey, ready, self._terminate, maxsize),
                name="master",
            )
            self.master.start()
            atexit.register(self.terminate)
            ready.wait()
        self.manager = AddressQueueManager(address=address, authkey=authkey)
        interval = 1.0
        for i in range(7):
            try:
                self.manager.connect()
                break
            except ConnectionError:
                time.sleep(interval)
                interval *= 1 + random.random()

        self.address_queue: Queue[Address] = self.manager.get_queue()
        self.qsize = self.address_queue.qsize
        self.empty = self.address_queue.empty
        self.full = self.address_queue.full

    def terminate(self):
        if self.master:
            self._terminate.set()
            self.master.join()

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        t = Timeout(timeout)
        while True:
            address = self.address_queue.get(block, t.remaining())
            try:
                with socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM
                ) as client:
                    client.connect(address)
                    with client.makefile("rb") as f:
                        return pickle.load(f)
            except ConnectionError:
                continue

    def put(
        self, x: T, block: bool = True, timeout: float | None = None
    ) -> None:
        t = Timeout(timeout)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind(("", 0))
            server.listen()
            port = server.getsockname()[1]
            self.address_queue.put((self.ip, port), block, t.remaining())
            server.settimeout(t.remaining())
            with server.accept()[0] as client:
                client.sendall(pickle.dumps(x))
