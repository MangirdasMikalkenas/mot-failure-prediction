# This code's only purpose is to check if all data in data/raw is downloaded in before
# starting to run other files. Last I checked (2026 November 11th) these are the names
# for the files in the data source.

from pathlib import Path

proj_root = Path(__file__).resolve.parents[3]  # src/mot/ingest goes to repo root
raw_dir = proj_root / "data" / "raw"
config_path = proj_root / "config" / "tunable.toml"


def expected_file_names(year: int) -> list(str):
    # Source filenames for one year. The publisher changed naming twice.
    if year <= 2016:
        return [f"test_item_{year}.txt.gz", f"test_results_{year}.txt.gz"]
    elif year <= 2023:
        return [f"dft_test_item_{year}.zip", f"dft_test_result_{year}.zip"]
    elif year == 2024:
        return [
            f"MOT+Testing+data+failure+item+({year}).zip",
            f"MOT+testing+data+results+({year}).zip",
        ]
    else:
        return "There are no such data files in original plan"
