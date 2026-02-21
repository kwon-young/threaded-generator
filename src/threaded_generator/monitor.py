"""
This module provides a monitoring tool for tracking the performance of threaded and
process-based generators.

It visualizes queue sizes, production rates, and consumption rates in real-time,
either in the terminal or using matplotlib.

Examples
--------
.. code-block:: python

    >>> from threaded_generator import ThreadedGenerator
    >>> import time
    >>>
    >>> # Create a monitor
    >>> monitor = Monitor()
    >>>
    >>> # Pass the monitor to a generator
    >>> gen = ThreadedGenerator(range(100), monitor=monitor, name="MyGen")
    >>>
    >>> # Start the monitor (usually in a context manager or separate thread)
    >>> with monitor:
    ...     for item in gen:
    ...         time.sleep(0.1)  # Simulate work
    ...
    # (Terminal output will show a progress bar and stats for "MyGen")
"""

from typing import Literal
import multiprocessing as mp
import multiprocessing.queues as mq
import threading as th
import time
from collections import defaultdict
import shutil


MonitorFormat = tuple[str, Literal["p", "c"], float, tuple[int, int], int, int]


class Monitor(th.Thread):
    """
    A monitoring thread that aggregates and displays statistics for parallel generators.

    It runs in the background, consuming statistics pushed by generators via a queue,
    and updates a live display in the terminal.

    :param name: The name of the monitor thread. Defaults to "Monitor".
    :type name: str
    """

    def __init__(self, name: str = "Monitor") -> None:
        super().__init__(name=name)
        self.stop = th.Event()
        self.nodes: set[str] = set()
        self.queue: mq.Queue[MonitorFormat] = mp.Queue()
        self.history: defaultdict[str, dict[str, list[float | int]]] = (
            defaultdict(lambda: {"t": [], "size": [], "p": [], "c": []})
        )
        self.start_time = time.monotonic()
        self.node_maxsizes: dict[str, int] = {}

    def put(self, x: MonitorFormat) -> None:
        """
        Push a statistic record to the monitor.

        This is typically called internally by the ``ParallelGenerator`` classes.

        :param x: A tuple containing (name, type, duration, thread_id, qsize, maxsize).
        :type x: MonitorFormat
        """
        self.queue.put(x)

    def drain(self) -> tuple[dict[tuple[str, str], float], dict[str, int]]:
        """
        Process all currently available items in the statistics queue.

        Calculates current throughputs and average queue sizes since the last drain.

        :return: A tuple of (throughput_map, average_queue_sizes).
        :rtype: tuple[dict[tuple[str, str], float], dict[str, int]]
        """
        node_deltas = defaultdict(list)
        node_qsizes = defaultdict(list)
        while not self.queue.empty():
            name, t, delta, tid, qsize, maxsize = self.queue.get()
            self.nodes.add(name)
            node_deltas[(name, t, tid)].append(delta)
            node_qsizes[name].append(qsize)
            self.node_maxsizes[name] = maxsize

        throughputs: defaultdict[tuple[str, str], float] = defaultdict(float)
        for (name, t, tid), ds in node_deltas.items():
            # Average duration per item for this thread
            avg_duration = sum(ds) / len(ds)
            if avg_duration > 0:
                # Throughput for this thread is 1 / duration
                throughputs[(name, t)] += 1.0 / avg_duration

        avg_qsizes = {}
        for name, sizes in node_qsizes.items():
            avg_qsizes[name] = int(round(sum(sizes) / len(sizes)))

        return throughputs, avg_qsizes

    def __enter__(self) -> "Monitor":
        """
        Start the monitor thread.

        :return: The monitor instance.
        :rtype: Monitor
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Signal the monitor thread to stop and wait for it to join.
        """
        self.stop.set()
        self.join()
        if self.nodes:
            # Move cursor down past the bars so we don't overwrite them
            # (assuming the last drawn frame is still on screen)
            print(f"\033[{len(self.nodes)}B", end="")

    def run(self) -> None:
        """
        Execute the main monitoring loop.

        Repeatedly drains the queue, calculates stats, and updates the terminal display
        until the stop event is set.
        """
        first = True
        last_lines = 0
        while not self.stop.is_set():
            throughputs, avg_qsizes = self.drain()
            now = time.monotonic() - self.start_time

            if not first and last_lines > 0:
                print(f"\033[{last_lines}A", end="\r")

            current_nodes = sorted(self.nodes)
            last_lines = len(current_nodes)

            for name in current_nodes:
                hist = self.history[name]
                hist["t"].append(now)
                qsize = avg_qsizes.get(name, 0)
                hist["size"].append(qsize)

                p_tps = throughputs.get((name, "p"), 0.0)
                c_tps = throughputs.get((name, "c"), 0.0)

                hist["p"].append(p_tps)
                hist["c"].append(c_tps)

                cols = shutil.get_terminal_size().columns
                display_name = (name or "")[:10].ljust(10)
                maxsize = self.node_maxsizes[name]
                stats = f" {qsize}/{maxsize} "
                stats += f"[P: {p_tps:5.1f}/s | C: {c_tps:5.1f}/s]"
                bar_len = max(0, cols - len(display_name) - len(stats) - 2)
                filled = int(bar_len * qsize / maxsize) if maxsize > 0 else 0
                bar = "█" * filled + "░" * (bar_len - filled)
                print(f"{display_name}|{bar}|{stats}")

            first = False
            time.sleep(1)

    def plot(self) -> None:
        """
        Display live statistics using matplotlib on the main thread.

        This method blocks until the plot window is closed. It requires `matplotlib`
        to be installed.
        """
        import matplotlib.pyplot as plt
        from matplotlib.animation import FuncAnimation

        # Set up plot
        fig = plt.figure()

        def update(frame) -> list:
            fig.clear()
            nodes = sorted(self.nodes)
            if not nodes:
                return []

            axes = fig.subplots(len(nodes), 1, sharex=True)
            if len(nodes) == 1:
                axes = [axes]

            for i, name in enumerate(nodes):
                hist = self.history[name]
                if not hist["t"]:
                    continue

                # Keep last 100 points for display
                limit = 100
                ts = hist["t"][-limit:]
                y_size = hist["size"][-limit:]
                y_p = hist["p"][-limit:]
                y_c = hist["c"][-limit:]

                maxsize = self.node_maxsizes.get(name, 0)

                ax = axes[i]
                ax.set_title(name)
                ax.plot(ts, y_size, label="Q Size")
                ax.axhline(
                    y=maxsize, color="r", linestyle="-", label="Q Maxsize"
                )
                ax.plot(ts, y_p, label="Prod/s")
                ax.plot(ts, y_c, label="Cons/s")
                ax.legend(loc="upper left")
            return []

        _ = FuncAnimation(fig, update, interval=1000, cache_frame_data=False)
        plt.show()
