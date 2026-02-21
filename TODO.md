# TODO v0.2.0

## New Examples

### Performance Demos
- [X] **Pipeline Throughput:** Implement a script comparing a standard generator chain vs. `ThreadedGenerator` (`Fast IO` -> `Slow CPU` -> `Fast IO`).
- [X] **Multiprocessing Fan-Out:** Implement an example running the pipeline defined above across multiple subprocesses using `multiprocessing.Pool` or `concurrent.futures`.
- [X] **Video Processing:** Implement a real-world pipeline: Download -> Decode (PyAV) -> Collate -> Transform (Numpy).

### Deep Learning Workflow
- [ ] **PyTorch Flexible Pipeline:** Create a training loop example without `torch.utils.data.DataLoader` (Infinite generator -> `ThreadedGenerator` -> Transforms -> `ThreadedGenerator` -> Batching -> Training).

## Code & Repository Improvements

### `tests/test_threaded_generator.py`
- [X] **Cleanup:** Remove unnecessary `sys.path` modifications if running with standard test runners.
- [ ] **Robustness:** Improve `test_pipeline_exception_propagation` to recursively find the root cause exception instead of relying on fixed wrapping depth.

### `pyproject.toml`
- [ ] **Version:** Bump version to `0.2.0` before release.
