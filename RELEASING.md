# Releasing a New Version

This project uses GitHub Actions to automatically publish releases to TestPyPI when a new tag is pushed.

## Step-by-Step Guide

1.  **Update the Version Number**

    Open `pyproject.toml` and update the version string to the new version number (e.g., `0.2.0`).

    ```toml
    [project]
    name = "threaded-generator"
    version = "0.2.0"  # <--- Update this line
    ```

2.  **Commit the Change**

    Commit the version bump to the repository.

    ```bash
    git add pyproject.toml
    git commit -m "Bump version to 0.2.0"
    git push
    ```

3.  **Create and Push a Tag**

    Create a git tag starting with `v` followed by the version number. Pushing this tag will trigger the GitHub Action workflow defined in `.github/workflows/publish_test.yml`.

    ```bash
    git tag v0.2.0
    git push origin v0.2.0
    ```

4.  **Verify the Release**

    Go to the "Actions" tab in the GitHub repository to monitor the build and publish process. Once completed, the new version should be available on TestPyPI.
