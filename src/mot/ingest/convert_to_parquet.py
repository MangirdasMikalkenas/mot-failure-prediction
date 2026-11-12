import zipfile
from pathlib import Path

import duckdb
import zipfile_deflate64  # noqa: F401 (importing it adds Deflate64 support to zipfile)

from mot.ingest.check_data_install import (
    expected_file_names,
    load_years,
    proj_root,
    raw_dir,
)

parquet_dir = proj_root / "data" / "parquet"
tmp_dir = proj_root / "data" / "tmp"


def list_csv_members(zip_path):
    csv_members = []

    with zipfile.ZipFile(zip_path, "r") as f_zip:
        for info in f_zip.infolist():
            if info.is_dir():
                continue
            if "__MACOSX" in info.filename:
                continue
            if info.filename.lower().endswith(".csv"):
                csv_members.append(info)

    csv_members.sort(key=lambda x: x.filename)

    return csv_members


def extract_csv(z, info, out_dir=tmp_dir):
    original_filename = info.filename
    info.filename = Path(info.filename).name
    try:
        extracted = z.extract(info, path=out_dir)
    finally:
        info.filename = original_filename
    return Path(extracted)


def csv_gz_to_parquet(csv_path, parquet_path):
    tmp_path = parquet_path.with_suffix(".parquet.tmp")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as con:
        con.execute("SET memory_limit = '5GB';")
        con.execute("SET preserve_insertion_order = false;")

        query = f"""
            COPY (SELECT * FROM read_csv('{csv_path}',
            header = true, all_varchar = true))
            TO '{tmp_path}'
            (FORMAT parquet, COMPRESSION zstd);
        """
        con.execute(query)
    tmp_path.rename(parquet_path)
