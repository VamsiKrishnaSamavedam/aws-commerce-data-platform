"""Create the dependency-free Lambda ZIP for Phase 6 orchestration."""

from argparse import ArgumentParser
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
FILES = (
    "scripts/register_incremental_batches.py",
    "scripts/operational_ledger_audit.py",
    "scripts/dispatch_dbt_build.py",
    "src/utils/batch_ledger.py",
    "src/utils/incremental_datasets.py",
    "src/utils/incremental_manifest.py",
)


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "dist" / "phase6_orchestrator.zip",
        help="ZIP destination; defaults to the historical Phase 6 artifact path.",
    )
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        for package in ("scripts", "src", "src/utils"):
            archive.writestr(f"{package}/__init__.py", "")
        for relative in FILES:
            archive.write(ROOT / relative, arcname=relative)
    print("Created:", output)


if __name__ == "__main__":
    main()
