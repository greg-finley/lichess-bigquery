import os

from google.cloud import storage

year_month = "2022-04"
variant = "racingKings"

storage_client = storage.Client()
bucket = storage_client.bucket("lichess-bigquery-pgn")


def os_run(command: str):
    """Run os commands but stop if error"""
    if os.system(command) != 0:
        raise Exception(f"Error with {command}")


os_run(
    f"curl https://database.lichess.org/{variant}/lichess_db_{variant}_rated_{year_month}.pgn.zst --output lichess_db_{variant}_rated_{year_month}.pgn.zst"
)

os_run(f"pzstd -d lichess_db_{variant}_rated_{year_month}.pgn.zst")

os_run(f"rm lichess_db_{variant}_rated_{year_month}.pgn.zst")

# Write to smaller files in 2000-game chunks, the first file being
# lichess_db_threeCheck_rated_2014-08_0001.pgn

num_games = 0
games_per_file = 2000
current_file_index = 1
current_file_name = (
    f"lichess_db_{variant}_rated_{year_month}_{current_file_index:04d}.pgn"
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
                current_file_name = f"lichess_db_{variant}_rated_{year_month}_{current_file_index:04d}.pgn"
                current_file = open(current_file_name, "w")

if num_games % games_per_file != 0:
    current_file.close()

os_run(f"rm lichess_db_{variant}_rated_{year_month}.pgn")

for i in range(1, current_file_index + 1):
    file_name = f"lichess_db_{variant}_rated_{year_month}_{i:04d}.pgn"
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    os_run(f"rm {file_name}")

print(f"Total number of games: {num_games}")
