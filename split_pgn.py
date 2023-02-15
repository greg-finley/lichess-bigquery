source_file = "lichess_db_threeCheck_rated_2014-09.pgn"

# Write to smaller files in 2000-game chunks, the first file being
# lichess_db_threeCheck_rated_2014-08_0001.pgn

num_games = 0
games_per_file = 2000
current_file_index = 1
current_file_name = f"lichess_db_threeCheck_rated_2014-09_{current_file_index:04d}.pgn"
current_file = open(current_file_name, "w")

with open(source_file) as g:
    for line in g:
        current_file.write(line)
        if line.startswith("1. "):
            num_games += 1
            if num_games % games_per_file == 0:
                current_file.close()
                current_file_index += 1
                current_file_name = (
                    f"lichess_db_threeCheck_rated_2014-09_{current_file_index:04d}.pgn"
                )
                current_file = open(current_file_name, "w")

if num_games % games_per_file != 0:
    current_file.close()

print(f"Total number of games: {num_games}")
