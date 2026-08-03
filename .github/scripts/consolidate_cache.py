import argparse
import hashlib
from pathlib import Path
from typing import Iterator, List

CACHE_READ_FILE_NAME = ".cache_write.ndjson"
CACHE_WRITE_FILE_NAME = ".cache.ndjson"


def find_cache_files(artifacts_dir: Path) -> List[Path]:
    """
    Find all diff cache files (".cache_write.ndjson") one level under the artifacts directory.
    """
    files = list(artifacts_dir.glob(f"*/{CACHE_READ_FILE_NAME}"))
    return sorted(files)


def _read_nonempty_lines(path: Path) -> Iterator[str]:
    if not path.exists() or not path.is_file():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                # Ensure each line ends with newline to prevent concatenation
                # when merging files (last line of a file may lack trailing \n)
                if not line.endswith("\n"):
                    line = line + "\n"
                yield line


def concatenate_files(
    input_files: List[Path],
    existing_cache_file: Path,
    output_file: Path,
) -> int:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Streamed rather than read into lists. Inputs here include the whole
    # upstream store as well as the base, and a cache entry is large -- the
    # median is around 100 KB -- so holding two full copies in memory would
    # scale with the store instead of with its entry count. Dedupe on a digest
    # per line: 32 bytes each, so the only resident structure stays in the
    # megabytes however far the store grows.
    seen: set[bytes] = set()
    written = 0

    # Written to a sibling first so a crash mid-merge cannot leave a truncated
    # store behind: the output is usually the base being read.
    staging = output_file.with_name(output_file.name + ".merging")
    with staging.open("w", encoding="utf-8") as out_f:
        # Base first, then the diffs, so first-seen order is preserved.
        for line in _read_nonempty_lines(existing_cache_file):
            digest = hashlib.sha256(line.encode("utf-8")).digest()
            if digest not in seen:
                seen.add(digest)
                out_f.write(line)
                written += 1

        for input_path in input_files:
            found = 0
            for line in _read_nonempty_lines(input_path):
                found += 1
                digest = hashlib.sha256(line.encode("utf-8")).digest()
                if digest not in seen:
                    seen.add(digest)
                    out_f.write(line)
                    written += 1
            print(f"Found {found} diff lines in {input_path}")

    staging.replace(output_file)

    # Indexed backends rebuild from fingerprint; drop any stale sidecar so
    # the next process does not serve offsets against the replaced file.
    for sidecar in (
        Path(f"{output_file}.idx"),
        Path(f"{CACHE_WRITE_FILE_NAME}.idx"),
        Path(f"{CACHE_READ_FILE_NAME}.idx"),
    ):
        if sidecar.exists():
            sidecar.unlink()
            print(f"Removed stale index sidecar: {sidecar}")

    return written


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge existing global cache with diff ndjson files from artifacts directory (unique lines only)",
    )
    parser.add_argument(
        "--artifacts-dir",
        required=True,
        type=Path,
        help="Path to directory where artifacts were downloaded",
    )
    args = parser.parse_args()

    artifacts_dir: Path = args.artifacts_dir
    output_file: Path = Path(CACHE_WRITE_FILE_NAME)

    cache_files: List[Path] = []
    if not artifacts_dir.exists() or not artifacts_dir.is_dir():
        # No artifacts uploaded (e.g., tests made no LLM calls) - this is OK
        print(f"No cache artifacts found (directory {artifacts_dir} does not exist)")
        print("This is normal for tests that don't make LLM calls.")
        # Ensure existing cache is preserved (if any)
        if output_file.exists():
            print(f"Existing cache preserved: {output_file}")
        return 0
    else:
        cache_files = find_cache_files(artifacts_dir)

    print("Discovered diff cache files to merge:")
    if cache_files:
        for path in cache_files:
            print(f"  {path}")
    else:
        print("  <none>")
        # No diff files but directory exists - preserve existing cache
        if output_file.exists():
            print(f"Existing cache preserved: {output_file}")
        return 0

    total_lines = concatenate_files(
        cache_files,
        existing_cache_file=output_file,
        output_file=output_file,
    )

    print(f"Wrote consolidated cache to: {output_file}")
    print(f"Total unique lines: {total_lines}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
