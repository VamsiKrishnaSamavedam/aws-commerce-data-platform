
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

SOURCES = (
    Path("src/utils/batch_ledger.py"),
    Path("src/utils/incremental_datasets.py"),
    Path("src/utils/incremental_manifest.py"),
)
OUTPUT = Path("dist/commerce_ledger_lib_phase6.zip")

for source in SOURCES:
    if not source.exists():
        raise FileNotFoundError(source)

OUTPUT.parent.mkdir(
    parents=True,
    exist_ok=True,
)

with ZipFile(OUTPUT, "w", ZIP_DEFLATED) as archive:
    archive.writestr("src/__init__.py", "")
    archive.writestr("src/utils/__init__.py", "")

    for source in SOURCES:
        archive.write(source, arcname=str(source).replace("\\", "/"))

print("Created:", OUTPUT)
