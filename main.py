import csv

file = "/Users/gregoryfinley/Downloads/lichess_db_racingKings_rated_2023-01.pgn"

num_games = 0
keys = {}

moves = []

game_str = "1. Kg3 { [%eval 0.19] [%clk 0:00:30] } 1... Kb3 { [%clk 0:00:29] } 2. Kf4 { [%clk 0:00:28] } 2... Kc4 { [%clk 0:00:27] } 3. Ke5 { [%clk 0:00:26] } 1-0"

ply_num = 0
game_split = game_str.split(" ")
for i, part in enumerate(game_split):
    if "." in part:
        ply_num += 1
        move = game_split[i + 1]
        clk = ""
        eval = ""
        try:
            if game_split[i + 2].startswith("{"):
                print("Hi!")
                j = i + 2

                while not game_split[j].endswith("}"):
                    print(game_split[j])
                    if game_split[j].startswith("[%clk"):
                        clk = game_split[j + 1].removesuffix("]")
                    elif game_split[j].startswith("[%eval"):
                        eval = game_split[j + 1].removesuffix("]")
                    j += 1

        except IndexError:
            pass

        moves.append((ply_num, move, clk, eval))


print(moves)

with open("/Users/gregoryfinley/Downloads/moves.csv", "w") as csv_file:
    writer = csv.writer(csv_file, delimiter=",")
    writer.writerow(["ply", "move", "clock", "eval"])
    with open(file) as f:
        for line in f:
            if line.startswith("1. "):
                # 1. e4 { [%eval 0.17] [%clk 0:00:30] } 1... c5 { [%eval 0.19] [%clk 0:00:30] }
                # 1. Kg3 { [%clk 0:00:30] } 1... Kb3 { [%clk 0:00:30] } 2. Kf4 { [%clk 0:00:29] } 2... Kc4 { [%clk 0:00:30] }
                # Split the line into plies

                num_games += 1
            elif line.startswith("["):
                key = line.split(" ")[0].removeprefix("[")
                if key in keys:
                    keys[key] += 1
                else:
                    keys[key] = 1

print(num_games)
print(keys)