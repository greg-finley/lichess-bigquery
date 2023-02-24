import os
from dataclasses import dataclass

import requests
from google.cloud import bigquery, storage
from torrentp import TorrentDownloader  # type: ignore


@dataclass
class VariantYearMonth:
    variant: str
    year_month: str


variants = [
    "atomic",
    "antichess",
    "chess960",
    "crazyhouse",
    "horde",
    "kingOfTheHill",
    "racingKings",
    # "standard",
    "threeCheck",
]

storage_client = storage.Client()
bucket = storage_client.bucket("lichess-bigquery-pgn")


def os_run(command: str):
    """Run os commands but stop if error"""
    if os.system(command) != 0:
        raise Exception(f"Error with {command}")


def split_pgn(variant_year_month: VariantYearMonth):
    variant = variant_year_month.variant
    year_month = variant_year_month.year_month

    print(f"Splitting {variant} {year_month}")

    os_run(
        f"curl https://database.lichess.org/{variant}/lichess_db_{variant}_rated_{year_month}.pgn.zst --output lichess_db_{variant}_rated_{year_month}.pgn.zst"
    )

    os_run(f"pzstd -d lichess_db_{variant}_rated_{year_month}.pgn.zst")

    os_run(f"rm lichess_db_{variant}_rated_{year_month}.pgn.zst")

    # Write to smaller files in 200000-game chunks, the first file being
    # lichess_db_threeCheck_rated_2014-08_00001.pgn

    num_games = 0
    games_per_file = 200000
    current_file_index = 1
    current_file_name = (
        f"lichess_db_{variant}_rated_{year_month}_{current_file_index:05d}.pgn"
    )
    current_file = open(current_file_name, "w")

    with open(f"lichess_db_{variant}_rated_{year_month}.pgn") as g:
        for line in g:
            current_file.write(line)
            if line.startswith("1. "):
                num_games += 1
                if num_games % games_per_file == 0:
                    current_file.close()
                    current_file_index += 1
                    current_file_name = f"lichess_db_{variant}_rated_{year_month}_{current_file_index:05d}.pgn"
                    current_file = open(current_file_name, "w")

    if num_games % games_per_file != 0:
        current_file.close()

    os_run(f"rm lichess_db_{variant}_rated_{year_month}.pgn")

    for i in range(1, current_file_index + 1):
        file_name = f"lichess_db_{variant}_rated_{year_month}_{i:05d}.pgn"
        blob = bucket.blob(file_name)
        print(f"Uploading {file_name}")
        blob.upload_from_filename(file_name)
        os_run(f"rm {file_name}")


existing_tables: list[VariantYearMonth] = [
    VariantYearMonth(
        variant=table.table_id.split("_")[1],
        year_month=table.table_id.split("_")[2] + "-" + table.table_id.split("_")[3],
    )
    for table in bigquery.Client().list_tables("lichess", max_results=100000)
    if table.table_id.startswith("games_")
]

for variant in variants:
    response = requests.get(f"https://database.lichess.org/{variant}/list.txt")
    assert response.status_code == 200
    # https://database.lichess.org/antichess/lichess_db_antichess_rated_2023-01.pgn.zst
    files: list[VariantYearMonth] = [
        VariantYearMonth(
            variant=item.split("_")[2],
            year_month=item.split("_")[4].split(".")[0],
        )
        for item in response.text.split("\n")
        if item != ""
    ]

    files = [file for file in files if file not in existing_tables]
    print(files)

    for file in files:
        split_pgn(file)

print("Done")
