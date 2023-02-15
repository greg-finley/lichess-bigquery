import datetime

import chess.pgn
from chess.engine import Cp, Mate, PovScore
from google.cloud import storage

"""A cloud function to process a PGN file and load it to BigQuery staging area"""


def ply_to_move(ply: int):
    """Converts a ply to a move number"""
    return int((ply + 1) / 2)


def ply_to_color(ply: int):
    """Converts a ply to a color"""
    return "White" if ply % 2 != 0 else "Black"


def clean_eval(eval: PovScore | None):
    if eval is None:
        return None
    else:
        eval_white = eval.white()
        if isinstance(eval_white, Cp):
            return f"{eval_white.cp / 100.0:.2f}"
        elif isinstance(eval_white, Mate):
            mate_moves = eval_white.mate()
            if mate_moves > 0:
                f"#{mate_moves}"
            else:
                f"-#{-mate_moves}"
        else:
            return None


def process_pgn(event, context):
    client = storage.Client()

    bucket = client.bucket("lichess-bigquery-pgn")
    blob = bucket.blob(event["name"])

    # event["name"] = "lichess_db_standard_rated_2023-01_0001.pgn"
    variant = event["name"].split("_")[2]
    year_month = event["name"].split("_")[4]

    # We need to collect the header keys for all games and tell BigQuery to
    # make the table with these columns all as strings
    game_header_keys: set[str] = set()
    game_header_keys.add("GameId")
    num_games = 0
    games = []
    moves = []

    with blob.open("rt") as pgn:
        while True:
            game = chess.pgn.read_game(pgn)  # type: ignore
            if not game:
                break

            num_games += 1
            if num_games % 500 == 0:
                print(
                    "Processing",
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    variant,
                    year_month,
                    num_games,
                )
            game_dict = {}
            for h in game.headers:
                game_dict[h] = game.headers[h]
                game_header_keys.add(h)
            game_dict["GameId"] = game_dict["Site"].split("/")[-1]
            games.append(game_dict)
            for node in game.mainline():
                ply = node.ply()
                board = node.board()

                moves.append(
                    [
                        game_dict["GameId"],
                        ply,
                        ply_to_move(ply),
                        ply_to_color(ply),
                        node.san(),
                        node.uci(),
                        node.clock(),
                        clean_eval(node.eval()),
                        board.fen(),
                        board.shredder_fen(),
                    ]
                )

    print("len(games)", len(games))
    print("len(moves)", len(moves))

    # Delete the blob
    blob.delete()
