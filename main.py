import csv
import os
import json
import datetime

ENDINGS = ["1-0\n", "0-1\n", "1/2-1/2\n"]


def parse_game(game_str):
    moves = []
    ply_num = 0
    game_split = game_str.split(" ")
    if not "..." in game_str:
        # Assume it's an old-style game, like
        # 1. Kh3 Rb4 2. Rg4 Be4 3. Kh4 0-1
        for i, part in enumerate(game_split):
            if "." in part:
                ply_num += 1
                move = game_split[i + 1]
                if move in ENDINGS:
                    break
                moves.append([ply_num, move, "", ""])
                # Try to add black's move. Might not be present at the end of the game
                try:
                    maybe_black_move = game_split[i + 2]
                    if maybe_black_move not in ENDINGS:
                        moves.append([ply_num, maybe_black_move, "", ""])
                except IndexError:
                    pass

    # Otherwise, assume it's a new-style game, like
    # 1. e4 { [%eval 0.17] [%clk 0:00:30] } 1... c5 { [%eval 0.19] [%clk 0:00:30] }
    else:
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


def os_run(command: str):
    """Run os commands but stop if error"""
    if os.system(command) != 0:
        raise Exception(f"Error with {command}")


def process_file(variant: str, year_month: str):
    print(
        "Starting",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        variant,
        year_month,
    )

    os_run(
        f"curl https://database.lichess.org/{variant}/lichess_db_{variant}_rated_{year_month}.pgn.zst --output lichess_db_{variant}_rated_{year_month}.pgn.zst"
    )

    os_run(f"pzstd -d lichess_db_{variant}_rated_{year_month}.pgn.zst")

    os_run(f"rm lichess_db_{variant}_rated_{year_month}.pgn.zst")

    pgn_filename = f"lichess_db_{variant}_rated_{year_month}.pgn"
    games_json_filename = f"games_{variant}_{year_month}.json"
    moves_csv_filename = f"moves_{variant}_{year_month}.csv"

    num_games = 0
    keys = {}

    with open(games_json_filename, "w") as games_json_file:
        with open(moves_csv_filename, "w") as moves_csv_file:
            csv_writer = csv.writer(moves_csv_file, delimiter=",")
            # csv_writer.writerow(["ply", "move", "clock", "eval", "game_id"])
            game_id = ""
            game = {}
            with open(pgn_filename) as pgn_file:
                for line in pgn_file:
                    if line.startswith("["):
                        if line.startswith("[Site"):
                            game_id = (
                                line.split(" ")[1]
                                .removeprefix('"https://lichess.org/')
                                .removesuffix('"]\n')
                            )
                        # line is [Event "Rated Racing Kings game"]
                        # key is Event
                        # value is Rated Racing Kings game
                        key = line.split(" ")[0].removeprefix("[")
                        value = (
                            line.removeprefix(f"[{key} ")
                            .removeprefix(
                                '"'
                            )  # Seems like always a quote here, but split into two removeprefixes just in case
                            .removesuffix("]\n")
                            .removesuffix('"')  # Ditto
                        )
                        game[key] = value

                        if key in keys:
                            keys[key] += 1
                        else:
                            keys[key] = 1

                    if line.startswith("1. "):
                        [
                            csv_writer.writerow(move + [game_id])
                            for move in parse_game(line)
                        ]
                        games_json_file.write(json.dumps(game) + "\n")

                        num_games += 1

    print("Num games", num_games)
    print("Key count", keys)

    # This will append if the table already exists
    os_run(
        f"bq load lichess.moves_{variant}_{year_month.replace('-', '_')} {moves_csv_filename} ply:integer,move:string,clock:string,eval:string,game_id:string"
    )
    os_run(
        f"bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect lichess.games_{variant}_{year_month.replace('-', '_')} {games_json_filename}"
    )

    os_run(f"rm {pgn_filename} {moves_csv_filename} {games_json_filename}")
    print(
        "Done",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        variant,
        year_month,
    )


process_file("racingKings", "2022-11")
