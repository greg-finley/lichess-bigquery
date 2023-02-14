source_file = "/Users/gregoryfinley/Downloads/lichess_db_racingKings_rated_2023-01.pgn"
out_file = "smaller.pgn"

# Write to a smaller file with the first 1000 games

num_games = 0

with open(out_file, "w") as f:
    with open(source_file) as g:
        for line in g:
            f.write(line)
            if line.startswith("1. "):
                num_games += 1
                if num_games >= 1000:
                    break
