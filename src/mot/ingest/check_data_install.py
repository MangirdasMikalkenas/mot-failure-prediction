# This code's only purpose is to check if all data in data/raw is downloaded in before
# starting to run other files. Last I checked (2026 November 11th) these are the names
# for the files in the data source. It does not check if some of the installations
# were cutoff.

import sys
import tomllib
from pathlib import Path

proj_root = Path(__file__).resolve().parents[3]  # src/mot/ingest goes to repo root
raw_dir = proj_root / "data" / "raw"
config_path = proj_root / "configs" / "tunable.toml"


def expected_file_names(year: int) -> list[str]:
    # Source filenames for one year. The publisher changed naming twice.
    if year <= 2016:
        return [f"test_item_{year}.txt.gz", f"test_result_{year}.txt.gz"]
    elif year <= 2023:
        return [f"dft_test_item_{year}.zip", f"dft_test_result_{year}.zip"]
    elif year == 2024:
        return [
            f"MOT+Testing+data+failure+item+({year}).zip",
            f"MOT+testing+data+results+({year}).zip",
        ]
    else:
        print(f"Take attention! No file names known for year (-s):\n{year}")
        return []


def load_years() -> range:
    with config_path.open("rb") as f:
        data_cfg = tomllib.load(f)["data"]
    # range() stops before the end number, so +1 makes last_year included.
    return range(data_cfg["first_year"], data_cfg["last_year"] + 1)


def ch_main() -> int:
    # Compares the files we expect with the files in data/raw.
    # Returns 1 if any file is missing, 0 if everything is there.
    # Read the [data] section of configs/tunable.toml to get the year range.
    # "rb" = read in binary mode, which tomllib needs.

    years = load_years()

    # Build one list with every file name we expect, for all years.
    expected = []
    for year in years:
        names_for_year = expected_file_names(year)
        for name in names_for_year:
            expected.append(name)

    # Keep the names that are not found in data/raw.
    missing = []

    for name in expected:
        # raw_dir / name joins the folder and file name into one full path.
        if not (raw_dir / name).is_file():
            missing.append(name)

    # An empty list counts as False, so this only runs if something is missing.
    if missing:
        print(
            f"Missing {len(missing)} of {len(expected)} data files "
            f"in {raw_dir} directory."
        )
        for name in missing:
            print(" ", name)
        return 1

    # If we got here, nothing was missing.
    print("yes, all data files are installed")
    return 0


if __name__ == "__main__":
    sys.exit(ch_main())
