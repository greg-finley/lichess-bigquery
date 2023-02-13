import csv
import datetime
import json
import os

import chess.pgn


def os_run(command: str):
    """Run os commands but stop if error"""
    if os.system(command) != 0:
        raise Exception(f"Error with {command}")


def ply_to_move(ply: int):
    """Converts a ply to a move number"""
    return int((ply + 1) / 2)


def ply_to_color(ply: int):
    """Converts a ply to a color"""
    return "White" if ply % 2 == 0 else "Black"


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

    with open(games_json_filename, "w") as games_json_file:
        with open(moves_csv_filename, "w") as moves_csv_file:
            csv_writer = csv.writer(moves_csv_file, delimiter=",")
            with open(pgn_filename) as pgn_file:
                while True:
                    game = chess.pgn.read_game(pgn_file)
                    if not game:
                        break

                    game_dict = {}
                    for h in game.headers:
                        game_dict[h] = game.headers[h]
                    game_dict["GameId"] = game_dict["Site"].split("/")[-1]
                    games_json_file.write(json.dumps(game_dict) + "\n")
                    for node in game.mainline():
                        csv_writer.writerow(
                            [
                                game_dict["GameId"],
                                node.ply(),
                                ply_to_move(node.ply()),
                                ply_to_color(node.ply()),
                                node.san(),
                                node.uci(),
                                node.clock(),
                                node.eval(),
                                node.board().fen(),
                                node.board().shredder_fen(),
                            ]
                        )

    # This will append if the table already exists
    # The moves schema is fixed, so we load as a CSV (highest BQ size limit)
    os_run(
        f"bq load lichess.moves_{variant}_{year_month.replace('-', '_')} {moves_csv_filename} game_id:string,ply:integer,move:integer,color:string,san:string,uci:string,clock:string,eval:string,fen:string,shredder_fen:string"
    )
    # Each game could have a variety of keys, and it could change over time. Load as JSON so the eventual table gets all possible keys
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


process_file("racingKings", "2016-01")
process_file("racingKings", "2023-01")
