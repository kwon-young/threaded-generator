import io
import logging
from itertools import batched
import multiprocessing as mp
import random
import requests
import av
import numpy as np
from typing import Iterator, Tuple, List, Generator, Iterable
from threaded_generator.threaded_generator import (
    ParallelGenerator,
    ThreadedGenerator,
    partial_generator,
    Monitor,
)

# Configuration
CLIP_DURATION = 2.0  # seconds
CLIP_FPS = 10  # frames per second
CLIP_FRAMES = int(CLIP_DURATION * CLIP_FPS)
VIDEO_URLS = [
    # Small sample videos from test-videos.co.uk
    "https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/360/Big_Buck_Bunny_360_10s_1MB.mp4",
    "https://test-videos.co.uk/vids/jellyfish/mp4/h264/360/Jellyfish_360_10s_1MB.mp4",
    "https://test-videos.co.uk/vids/bear/mp4/h264/360/Bear_360_10s_1MB.mp4",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


@partial_generator
def download_videos(urls: List[str]) -> Iterator[Tuple[str, io.BytesIO]]:
    """Downloads videos into memory."""
    for url in urls:
        filename = url.split("/")[-1]
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = io.BytesIO(response.content)
            yield (filename, data)
        except Exception:
            pass


@partial_generator
def decode_frames(container, stream) -> Iterator[av.VideoFrame]:
    """Yields frames from the container stream."""
    for frame in container.decode(stream):
        yield frame


@partial_generator
def sample_frames(
    frames: Iterator[av.VideoFrame], video_fps: float, target_fps: float
) -> Iterator[Tuple[float, np.ndarray]]:
    """
    Samples frames to match the target FPS.
    Yields (timestamp, numpy arrays (H, W, C)).
    """
    step = video_fps / target_fps
    current_pos = 0.0

    for i, frame in enumerate(frames):
        # Simple nearest neighbor sampling
        if i >= int(current_pos):
            # frame.time is the timestamp in seconds
            yield (frame.time, frame.to_ndarray(format="rgb24"))
            current_pos += step


@partial_generator
def crop_frames(
    frames: Iterator[Tuple[float, np.ndarray]], size: int = 125
) -> Iterator[Tuple[float, np.ndarray]]:
    """
    Crops the top-left corner of frames.
    """
    for ts, img in frames:
        # Top-left crop: img[0:size, 0:size, :]
        yield (ts, img[:size, :size, :])


TimeSpan = tuple[float, float]


@partial_generator
def collate_clips(
    videos: Iterator[Tuple[str, io.BytesIO]],
) -> Iterator[np.ndarray]:
    """
    Opens videos, decodes linearly, samples, and collates clips based on random timestamps.
    """
    for filename, data in videos:
        try:
            container = av.open(data)
            stream = container.streams.video[0]
            stream.thread_type = "AUTO"
            stream.thread_count = 1

            # Get video metadata
            if stream.average_rate:
                video_fps = float(stream.average_rate)
            else:
                video_fps = 24.0  # Fallback

            duration_sec = float(stream.duration * stream.time_base)

            # Generate random start times
            start_times = np.arange(0, duration_sec, CLIP_DURATION)
            end_times = start_times + CLIP_DURATION
            clip_times = np.stack([start_times, end_times]).T.tolist()

            # Prepare clip buffers
            clips: list[tuple[TimeSpan, list[np.ndarray]]] = [
                ((start, end), []) for (start, end) in clip_times
            ]

            # Decode all frames linearly
            decoded = decode_frames(container, stream)

            # Sample frames to match target CLIP_FPS
            sampled = sample_frames(decoded, video_fps, CLIP_FPS)

            # Crop frames
            cropped = crop_frames(sampled)

            for ts, img in cropped:
                for (start, end), clip in clips:
                    if start > ts:
                        break
                    # Check if frame belongs to this clip
                    if start <= ts and len(clip) < CLIP_FRAMES:
                        clip.append(img)
                i = 0
                while i < len(clips):
                    if len(clips[i][1]) == CLIP_FRAMES:
                        _, clip = clips.pop(i)
                        yield np.stack(clip)
                    else:
                        i += 1
            for _, clip in clips:
                clip.extend(clip[-1:] * (CLIP_FRAMES - len(clip)))
                yield np.stack(clip)

            container.close()

        except Exception:
            pass


@partial_generator
def transform_clips(clips: Iterator[np.ndarray]) -> Iterator[np.ndarray]:
    """Applies heavy numpy transformations to clips."""
    for clip in clips:
        # Simulate heavy processing: e.g. 3D convolution or heavy augs
        # Rotate 90 degrees: (F, H, W, C) -> (F, W, H, C)
        transformed = np.rot90(clip, axes=(1, 2))

        # Artificial CPU load
        transformed = transformed.astype(np.float32) / 255.0
        transformed = transformed**2.2  # Gamma correction simulation

        yield transformed


@partial_generator
def video_pipeline(
    use_threads: bool, urls: Iterable[str] = VIDEO_URLS, monitor=None
) -> Generator:
    disable = not use_threads

    # 1. Download (Network Bound)
    # ThreadedGenerator helps here by pre-fetching the next video
    # while the current one is being decoded.
    videos = download_videos(urls)
    videos = ThreadedGenerator(
        videos, maxsize=5, disable=disable, name="Download", monitor=monitor
    )

    # 2. Decode (CPU/FFmpeg Bound)
    # Decodes bytes to numpy clips.
    clips = collate_clips(videos)

    # 4. Transform (Heavy CPU/GPU Bound)
    yield from transform_clips(clips)


def collate(clips, batch_size):
    for batch in batched(clips, batch_size):
        yield np.stack(batch)


def data_loader(
    batch_size: int,
    use_threads: bool,
    workers: int,
    urls: list[str],
    monitor: Monitor,
) -> Generator:
    urls = random.sample(urls, len(urls))
    urls = ThreadedGenerator(
        urls, queue_type=mp.Queue, name="Urls", monitor=monitor
    )
    clips = ParallelGenerator(
        video_pipeline(use_threads, urls, monitor),
        maxsize=batch_size * workers,
        num_workers=workers,
        name="Pipeline",
        monitor=monitor,
    )
    batch = collate(clips, batch_size)
    yield from ThreadedGenerator(batch, name="Collate", monitor=monitor)


def main():
    print("=== Video Processing Pipeline Demo ===")
    print("Dependencies: requests (Network), av (Decode), numpy (Transform)\n")

    urls = VIDEO_URLS * 30 * 5
    use_threads = True
    monitor = Monitor()
    with monitor:
        for batch in data_loader(4, use_threads, 4, urls, monitor):
            pass
    monitor.plot()


if __name__ == "__main__":
    main()
