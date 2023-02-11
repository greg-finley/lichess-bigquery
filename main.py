import csv
import os

os.system(
    "curl https://database.lichess.org/racingKings/lichess_db_racingKings_rated_2023-01.pgn.zst -o lichess_db_racingKings_rated_2023-01.pgn.zst"
)

os.system("pzstd -d lichess_db_racingKings_rated_2023-01.pgn.zst")

file = "lichess_db_racingKings_rated_2023-01.pgn"

num_games = 0
keys = {}

moves = []

game_str = "1. Kg3 { [%eval 0.19] [%clk 0:00:30] } 1... Kb3 { [%clk 0:00:29] } 2. Kf4 { [%clk 0:00:28] } 2... Kc4 { [%clk 0:00:27] } 3. Ke5 { [%clk 0:00:26] } 1-0"


def parse_game(game_str):
    moves = []
    ply_num = 0
    game_split = game_str.split(" ")
    for i, part in enumerate(game_split):
        if "." in part and not part.endswith("]"):
            ply_num += 1
            move = game_split[i + 1]
            clk = ""
            eval = ""
            try:
                if game_split[i + 2].startswith("{"):
                    j = i + 2
                    while not game_split[j].endswith("}"):
                        if game_split[j].startswith("[%clk"):
                            clk = game_split[j + 1].removesuffix("]")
                        elif game_split[j].startswith("[%eval"):
                            eval = game_split[j + 1].removesuffix("]")
                        j += 1

            except IndexError:
                pass

            moves.append([ply_num, move, clk, eval])

    return moves


print(parse_game(game_str))

with open("games.json", "w") as games_json_file:
    with open("moves.csv", "w") as moves_csv_file:
        csv_writer = csv.writer(moves_csv_file, delimiter=",")
        # csv_writer.writerow(["ply", "move", "clock", "eval", "game_id"])
        game_id = ""
        game = {}
        with open(file) as f:
            for line in f:
                if line.startswith("["):
                    if line.startswith("[Site"):
                        game_id = (
                            line.split(" ")[1]
                            .removeprefix('"https://lichess.org/')
                            .removesuffix('"]\n')
                        )
                    key = line.split(" ")[0].removeprefix("[")
                    value = line.split(" ")[1].removesuffix("\n")
                    game[key] = value
                    if key in keys:
                        keys[key] += 1
                    else:
                        keys[key] = 1

                if line.startswith("1. "):
                    [csv_writer.writerow(move + [game_id]) for move in parse_game(line)]
                    games_json_file.write(str(game) + "\n")

                    num_games += 1


print(num_games)
print(keys)

# This will append if the table already exists
os.system(
    "bq load lichess.moves_python moves.csv ply:integer,move:string,clock:string,eval:string,game_id:string"
)
os.system(
    "bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect lichess.games_python games.json"
)

os.system(
    "rm lichess_db_racingKings_rated_2023-01.pgn.zst lichess_db_racingKings_rated_2023-01.pgn moves.csv games.json"
)
