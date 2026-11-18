# Converts the raw MOT files in data/raw into parquet files in data/parquet.
# Every year gets its own folder, for example data/parquet/test_result/year=2020/
# When a year is finished, an empty file called _DONE is put in its folder,
# so the next run (if needed) can skip that year.

import gzip
import sys
import time
import zipfile
from pathlib import Path

import duckdb

# Never use zipfile_deflate64 by name. Just importing it enables zipfile
# to open "Deflate64" zips, some of the MOT zips are that.
import zipfile_deflate64  # noqa: F401

from mot.ingest.check_data_install import (
    ch_main,
    expected_file_names,
    load_years,
    proj_root,
    raw_dir,
)

parquet_dir = proj_root / "data" / "parquet"
tmp_dir = proj_root / "data" / "tmp"

# How many unreadable lines we accept in one file before we stop.
max_bad_lines = 10


def list_csv_members(z):
    # Make a list with the names of all CSV files inside an open zip file.
    csv_names = []

    for name in z.namelist():
        # Folder names end with "/"
        if name.endswith("/"):
            continue
        # Zips made on a Mac have an extra __MACOSX folder with junk files in it.
        if "__MACOSX" in name:
            continue
        if name.lower().endswith(".csv"):
            csv_names.append(name)

    csv_names.sort()
    return csv_names


def extract_csv(z, name):
    # Copy one CSV file out of the zip into data/tmp and give back its path.
    # Some zips keep the CSV inside a folder. Path(name).name keeps only the
    # file name, so the CSV always lands straight in data/tmp.
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_path = tmp_dir / Path(name).name

    # A CSV can be many GB, so we copy it in pieces of 10 MB
    # instead of loading the whole file into memory at once.
    # One "with" can open two files: source (in the zip) and target (on disk).
    with z.open(name) as source, open(out_path, "wb") as target:
        while True:
            piece = source.read(10_000_000)
            if len(piece) == 0:
                break  # nothing left to read
            target.write(piece)

    return out_path


def count_lines(f):
    # Count the lines in an open file, and count how many \" it has.
    # The file is opened in "rb" (binary) mode, so every line is bytes, not text.
    # That is it searches for b'\\"': the b means bytes, so it's
    # looking for the 2 characters \"
    n_lines = 0
    n_backslash_quotes = 0
    for line in f:
        n_lines += 1
        n_backslash_quotes += line.count(b'\\"')

    # The first line is the header
    n_rows = n_lines - 1
    return n_rows, n_backslash_quotes


def scan_csv(csv_path):
    # .gz files are compressed, so they need gzip.open instead of open.
    if csv_path.name.endswith(".gz"):
        with gzip.open(csv_path, "rb") as f:
            n_rows, n_backslash_quotes = count_lines(f)
    else:
        with open(csv_path, "rb") as f:
            n_rows, n_backslash_quotes = count_lines(f)

    return n_rows, n_backslash_quotes


# There are two ways a CSV file can write a " that is inside a value:
#   normal way:     "88"" 4 CYL"   (DuckDB finds this way by itself)
#   backslash way:  "88\" 4 CYL"   (DuckDB needs to be informed about this way)


def try_to_convert(csv_path, tmp_path, way):
    # Convert the CSV file to a parquet file one time.
    # way is "normal" or "backslash".
    # Gives back rows written, and lines DuckDB could not read.

    # For the backslash way we add extra options to read_csv.
    # After Python reads the escapes, DuckDB sees:  , quote = '"', escape = '\'
    extra_options = ""
    if way == "backslash":
        extra_options = ", quote = '\"', escape = '\\'"

    con = duckdb.connect()
    con.execute("SET memory_limit = '5GB';")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET enable_progress_bar = false;")

    # all_varchar = true: read every column as text, so DuckDB doesn't guess types.
    # store_rejects = true: lines DuckDB can't read are saved in a table called
    # reject_errors, instead of crashing the program.
    query = f"""
        COPY (SELECT * FROM read_csv('{csv_path}',
        header = true, all_varchar = true, store_rejects = true{extra_options}))
        TO '{tmp_path}'
        (FORMAT parquet, COMPRESSION zstd);
    """
    con.execute(query)

    # fetchall() gives a list of rows. [0][0] is the first value of the first row.
    result = con.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}')").fetchall()
    rows_written = result[0][0]

    result = con.sql("SELECT count(DISTINCT line) FROM reject_errors").fetchall()
    bad_lines = result[0][0]

    # Where to save the lines DuckDB could not read.
    # Example: part_00.parquet.tmp  ->  part_00_rejects.csv
    rejects_name = tmp_path.name.replace(".parquet.tmp", "_rejects.csv")
    rejects_path = tmp_path.parent / rejects_name

    # Delete the file from an earlier try, so it never shows old lines.
    if rejects_path.exists():
        rejects_path.unlink()

    if bad_lines > 0:
        con.execute(f"COPY reject_errors TO '{rejects_path}' (HEADER)")

    con.close()
    return rows_written, bad_lines


def check_columns(parquet_path, first_columns):
    # Read the column names of the parquet file we just made.
    con = duckdb.connect()
    rows = con.sql(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()
    con.close()

    columns = []
    for row in rows:
        columns.append(row[0])  # row[0] is the column name

    # A good MOT file has many columns and starts with test_id.
    # If not, DuckDB probably guessed the wrong delimiter.
    # "raise" stops the whole program and prints the message.
    if len(columns) < 5:
        raise RuntimeError(f"{parquet_path}: too few columns: {columns}")
    if columns[0] != "test_id":
        raise RuntimeError(f"{parquet_path}: first column is not test_id: {columns}")

    # Every part of one year must have the same columns as part 0.
    # first_columns is empty for part 0, so this check is skipped there.
    if len(first_columns) > 0 and columns != first_columns:
        raise RuntimeError(
            f"{parquet_path}: columns {columns} "
            f"are not the same as part 0: {first_columns}"
        )

    return columns


def check_year(table, year):
    # Look for duplicates and strange dates in one finished year folder.
    folder = parquet_dir / table / f"year={year}"
    # *.parquet means "every .parquet file in this folder".
    files = f"{folder}/*.parquet"

    con = duckdb.connect()
    con.execute("SET memory_limit = '5GB';")

    # 1. How many rows are there, and how many different rows?
    result = con.sql(f"SELECT count(*) FROM read_parquet('{files}')").fetchall()
    total = result[0][0]

    query = f"SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet('{files}'))"
    result = con.sql(query).fetchall()
    different = result[0][0]

    if total != different:
        n_copies = total - different
        print(f"  WARNING: {year} {table}: {n_copies} rows are exact copies")

    # 2. Only for test_result: each test_id should appear one time,
    #    and we print the first and last date so we can see they fit the year.
    if table == "test_result":
        query = f"SELECT count(DISTINCT test_id) FROM read_parquet('{files}')"
        result = con.sql(query).fetchall()
        different_ids = result[0][0]

        if total != different_ids:
            n_repeats = total - different_ids
            print(f"  WARNING: {year} test_result: {n_repeats} test_id values repeat")

        query = f"SELECT min(test_date), max(test_date) FROM read_parquet('{files}')"
        result = con.sql(query).fetchall()
        first_date = result[0][0]
        last_date = result[0][1]
        print(f"  {year} test_result: dates from {first_date} to {last_date}")

    con.close()


def csv_gz_to_parquet(csv_path, parquet_path):
    # Convert one CSV (or .gz) file into one parquet file.
    # We first write a .tmp file and only rename it to .parquet when we know
    # it is good, so a half-finished file never looks like a finished one.
    tmp_path = parquet_path.parent / (parquet_path.name + ".tmp")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # How many rows the file should give, and how many \" it has.
    expected_rows, backslash_quotes = scan_csv(csv_path)

    # Choose which way to try first. If the file has \" in it,
    # the backslash way is probably right, so try that one first.
    first_way = "normal"
    second_way = "backslash"
    if backslash_quotes > 0:
        first_way = "backslash"
        second_way = "normal"

    # Try 1
    rows_1, bad_1 = try_to_convert(csv_path, tmp_path, first_way)
    if rows_1 == expected_rows:
        # Every line became a row. Perfect, keep it.
        tmp_path.rename(parquet_path)
        return

    # Try 2
    rows_2, bad_2 = try_to_convert(csv_path, tmp_path, second_way)
    if rows_2 == expected_rows:
        # Every line became a row. Perfect, keep it.
        tmp_path.rename(parquet_path)
        return

    # No try was perfect. A try is still OK if:
    #   every line is a row OR a bad line that DuckDB told us about,
    #   and there are only a few bad lines.
    try_1_ok = False
    if rows_1 + bad_1 == expected_rows and bad_1 <= max_bad_lines:
        try_1_ok = True

    try_2_ok = False
    if rows_2 + bad_2 == expected_rows and bad_2 <= max_bad_lines:
        try_2_ok = True

    if try_1_ok:
        # The .tmp file has try 2 in it now, so make try 1 again.
        try_to_convert(csv_path, tmp_path, first_way)
        print(f"  WARNING: skipped {bad_1} bad lines in {csv_path.name}")
        tmp_path.rename(parquet_path)
        return

    if try_2_ok:
        # The .tmp file already has try 2 in it, so just keep it.
        print(f"  WARNING: skipped {bad_2} bad lines in {csv_path.name}")
        tmp_path.rename(parquet_path)
        return

    # Rows went missing in both tries. Stop the whole program.
    raise RuntimeError(
        f"{csv_path.name}: {expected_rows} rows expected, but try 1 gave {rows_1} "
        f"rows and try 2 gave {rows_2} rows. Rows went missing!"
    )


def convert_archive(archive_path, table, year):
    # Convert one raw file (a .gz or a .zip) into the parquet files for one year.
    # table is "test_item" or "test_result".
    out_folder = parquet_dir / table / f"year={year}"
    done_marker = out_folder / "_DONE"

    if done_marker.exists():
        print(f"Skipping {year} ({table}): already done.")
        return

    out_folder.mkdir(parents=True, exist_ok=True)

    if archive_path.name.endswith(".gz"):
        # A .gz file has only one CSV in it, so it becomes just part 0.
        parquet_path = out_folder / "part_00.parquet"
        csv_gz_to_parquet(archive_path, parquet_path)
        # [] means there is no part 0 to compare the columns with.
        check_columns(parquet_path, [])

    elif archive_path.name.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as z:
            csv_names = list_csv_members(z)
            if len(csv_names) == 0:
                raise RuntimeError(f"{archive_path.name} has no CSV files in it")

            # Column names of part 0. Empty until the first part is done.
            first_columns = []

            # part goes 0, 1, 2, ... one number for each CSV in the zip.
            for part in range(len(csv_names)):
                name = csv_names[part]
                csv_path = extract_csv(z, name)

                # zfill(2) puts a 0 in front of short numbers: 3 -> "03"
                part_name = "part_" + str(part).zfill(2) + ".parquet"
                parquet_path = out_folder / part_name

                try:
                    csv_gz_to_parquet(csv_path, parquet_path)
                finally:
                    # "finally" runs even if the line above crashes,
                    # so the big extracted CSV is always deleted.
                    csv_path.unlink()

                first_columns = check_columns(parquet_path, first_columns)

    else:
        raise ValueError(f"Don't know how to convert {archive_path.name}")

    # Look for duplicates and strange dates. This only prints warnings.
    check_year(table, year)

    # Make the empty _DONE file, so the next run skips this year.
    done_marker.touch()

    print(f"Successfully processed {year} ({table}).")


def main():
    start = time.time()

    if ch_main() == 1:
        print("Some raw files are missing, stopping.")
        return 1

    for year in load_years():
        names = expected_file_names(year)

        # expected_file_names gives back an empty list for years it doesn't know.
        if len(names) != 2:
            print(f"No file names known for {year}, skipping.")
            continue

        # names[0] is the test_item file, names[1] is the test_result file.
        convert_archive(raw_dir / names[0], "test_item", year)
        convert_archive(raw_dir / names[1], "test_result", year)

    end = time.time()
    minutes = round((end - start) / 60, 1)
    print(f"The conversion took {minutes} minutes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
