import time
import io
import logging
import multiprocessing
import requests
import av
import numpy as np
from typing import Iterator, Tuple, List
from threaded_generator import ThreadedGenerator, Monitor

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


def download_videos(urls: List[str]) -> Iterator[Tuple[str, io.BytesIO]]:
    """Downloads videos into memory."""
    for url in urls:
        filename = url.split("/")[-1]
        t0 = time.time()
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = io.BytesIO(response.content)
            yield (filename, data)
        except Exception as e:
            pass


def decode_frames(container, stream) -> Iterator[av.VideoFrame]:
    """Yields frames from the container stream."""
    for frame in container.decode(stream):
        yield frame


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
            num_clips = max(1, int(duration_sec / CLIP_DURATION))

            # Generate random start times
            start_times = np.arange(0, duration_sec, CLIP_DURATION)
            end_times = start_times + CLIP_DURATION
            clip_times = np.stack([start_times, end_times]).T.tolist()

            # Prepare clip buffers
            clips: list[tuple[TimeSpan, list[np.ndarray]]] = [
                ((start, end), []) for (start, end) in clip_times]

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
                    if start <= ts < end:
                        clip.append(img)
                i = 0
                while i < len(clips):
                    if clips[i][0][1] < ts:
                        _, clip = clips.pop(i)
                        if clip:
                            yield np.stack(clip)
                    else:
                        i += 1
            for _, clip in clips:
                yield np.stack(clip)

            container.close()

        except Exception as e:
            pass


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


def measure_throughput(gen: Iterator, name: str) -> Iterator:
    start = time.time()
    count = 0
    for item in gen:
        yield item
        count += 1
    duration = time.time() - start
    speed = count / duration if duration > 0 else 0


def run_pipeline(use_threads: bool, urls: List[str] = VIDEO_URLS, monitor=None) -> float:
    logger.info(f"Starting pipeline (Threaded={use_threads}, Items={len(urls)})")
    start = time.time()
    start_cpu = time.process_time()

    disable = not use_threads

    # 1. Download (Network Bound)
    # ThreadedGenerator helps here by pre-fetching the next video
    # while the current one is being decoded.
    videos = download_videos(urls)
    videos_gen = ThreadedGenerator(
        videos, maxsize=5, disable=disable, monitor=monitor, name="Download"
    )

    # 2. Decode (CPU/FFmpeg Bound)
    # Decodes bytes to numpy clips.
    clips = collate_clips(videos_gen)

    # 3. Buffer decoded clips before heavy processing
    # If transform is slower than decode, this buffer fills up.
    # If decode is slower, this keeps transform fed immediately.
    clips_gen = ThreadedGenerator(
        clips, maxsize=5, disable=disable, monitor=monitor, name="Decode"
    )

    # 4. Transform (Heavy CPU/GPU Bound)
    final_clips = transform_clips(clips_gen)
    # final_clips = clips_gen

    count = 0
    for _ in final_clips:
        count += 1

    duration = time.time() - start
    cpu_duration = time.process_time() - start_cpu
    saturation = (cpu_duration / duration * 100) if duration > 0 else 0.0

    speed = count / duration if duration > 0 else 0.0
    logger.info(
        f"Total time: {duration:.2f}s | CPU time: {cpu_duration:.2f}s | "
        f"Saturation: {saturation:.1f}% | Clips: {count} | Speed: {speed:.2f} clips/s"
    )
    return speed


def run_worker_pipeline(args) -> float:
    """Worker function for multiprocessing."""
    use_threads, urls = args
    # Configure logging for worker process
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s - [Worker] %(message)s", force=True)
    return run_pipeline(use_threads, urls)


def run_mp_benchmark(use_threads: bool, workers: int, urls: list[str]) -> float:
    name = "Threaded" if use_threads else "Standard"
    logger.info(f"Running {name} Pipeline with {workers} processes...")

    # Split work
    chunk_size = len(urls) // workers
    chunks = [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]
    # Handle remainder
    if len(chunks) > workers:
        chunks[-2].extend(chunks[-1])
        chunks.pop()

    start_total = time.time()

    with multiprocessing.Pool(processes=workers) as pool:
        speeds = pool.map(run_worker_pipeline, [(use_threads, chunk) for chunk in chunks])

    total_duration = time.time() - start_total
    total_speed = sum(speeds) # Sum of speeds from all workers

    logger.info(f"{name} MP Total Duration: {total_duration:.2f}s | Aggregate Speed: {total_speed:.2f} clips/s")
    return total_speed


def main():
    print("=== Video Processing Pipeline Demo ===")
    print("Dependencies: requests (Network), av (Decode), numpy (Transform)\n")

    # print("\n--- Single Process Benchmark ---\n")
    #
    # logger.info("Running Standard Pipeline...")
    # speed_std = run_pipeline(use_threads=False, urls=VIDEO_URLS*3)
    #
    # print("-" * 30)
    #
    # logger.info("Running Threaded Pipeline...")
    # with Monitor() as monitor:
    #     speed_thread = run_pipeline(use_threads=True, urls=VIDEO_URLS*3, monitor=monitor)
    #
    # print("-" * 30)
    # if speed_std > 0:
    #     improvement = (speed_thread - speed_std) / speed_std * 100
    #     print(f"Improvement: {improvement:.1f}%")
    # else:
    #     print("Improvement: N/A")

    print("\n--- Multiprocessing Benchmark (2 Processes) ---\n")
    
    speed_std_mp = run_mp_benchmark(use_threads=False, workers=5,
                                    urls=VIDEO_URLS*6*2)
    print("-" * 30)
    speed_thread_mp = run_mp_benchmark(use_threads=True, workers=5,
                                       urls=VIDEO_URLS*6*2)
    
    print("-" * 30)
    if speed_std_mp > 0:
        improvement_mp = (speed_thread_mp - speed_std_mp) / speed_std_mp * 100
        print(f"MP Improvement: {improvement_mp:.1f}%")
    else:
        print("MP Improvement: N/A")


if __name__ == "__main__":
    main()
