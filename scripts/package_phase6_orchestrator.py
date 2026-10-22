"""Create the dependency-free Lambda ZIP for Phase 6 orchestration."""

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "dist" / "phase6_orchestrator.zip"
FILES = (
    "scripts/register_incremental_batches.py",
    "src/utils/batch_ledger.py",
    "src/utils/incremental_datasets.py",
    "src/utils/incremental_manifest.py",
)


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(OUTPUT, "w", ZIP_DEFLATED) as archive:
        for package in ("scripts", "src", "src/utils"):
            archive.writestr(f"{package}/__init__.py", "")
        for relative in FILES:
            archive.write(ROOT / relative, arcname=relative)
    print("Created:", OUTPUT)


if __name__ == "__main__":
    main()
