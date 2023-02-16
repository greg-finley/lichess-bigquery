from __future__ import annotations

import csv
import json
import os
from io import StringIO, TextIOWrapper
from typing import Any

import chess.pgn
from chess.engine import Cp, Mate, PovScore
from google.cloud import storage

"""
A cloud function to process a PGN file from GCS and upload to GCS files for its games and moves.

This file is ok to run locally but it will hit a different cloud bucket.
"""


Data = list[dict[str, Any]]
storage_client = storage.Client()
game_bucket_name = "lichess-game-json"
game_bucket = storage_client.bucket(game_bucket_name)
move_bucket_name = "lichess-move-csv"
move_bucket = storage_client.bucket(move_bucket_name)


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
    is_local = event.get("isLocal", False)
    event_name = event["name"]

    # event["name"] = "lichess_db_standard_rated_2023-01_0001.pgn"
    variant = event_name.split("_")[2]
    year_month = event_name.split("_")[4]
    file_suffix = event_name.split("_")[5].split(".")[0]

    # We need to collect the header keys for all games and tell BigQuery to
    # make the table with these columns all as strings
    game_header_keys: set[str] = set()
    game_header_keys.add("GameId")
    # Add the default headers from python-chess
    for h in chess.pgn.Headers().keys():
        game_header_keys.add(h)
    num_games = 0
    games: Data = []
    moves: Data = []
    bucket_name = (
        "lichess-bigquery-pgn" if not is_local else "lichess-bigquery-pgn-local"
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(event_name)

    # Do one quick pass through the pgn to get the header keys.
    # Needed to create BigQuery table with the right schema
    with blob.open("rt") as pgn:
        assert isinstance(pgn, TextIOWrapper)
        for line in pgn:
            if line.startswith("["):
                key = line.split(" ")[0].strip("[")
                game_header_keys.add(key)

    with blob.open("rt") as pgn:
        assert isinstance(pgn, TextIOWrapper)
        while True:
            game = chess.pgn.read_game(pgn)
            if not game:
                break

            num_games += 1
            game_dict = {}
            for h in game.headers:
                game_dict[h] = game.headers[h]
            game_dict["GameId"] = game_dict["Site"].split("/")[-1]
            # for any missing header keys, add them with a null value
            for k in game_header_keys:
                if k not in game_dict:
                    game_dict[k] = None
            games.append(game_dict)
            for node in game.mainline():
                ply = node.ply()
                board = node.board()

                moves.append(
                    {
                        "game_id": game_dict["GameId"],
                        "ply": ply,
                        "move": ply_to_move(ply),
                        "color": ply_to_color(ply),
                        "san": node.san(),
                        "uci": node.uci(),
                        "clock": node.clock(),
                        "eval": clean_eval(node.eval()),
                        "fen": board.fen(),
                        "shredder_fen": board.shredder_fen(),
                    }
                )

    # Store the new objects
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=moves[0].keys())

    # Write each row to the CSV
    for m in moves:
        writer.writerow(m)

    ndjson = "\n".join(json.dumps(d) for d in games)

    game_blob = game_bucket.blob(f"{variant}_{year_month}_{file_suffix}.json")
    game_blob.upload_from_string(
        ndjson.encode("utf-8"), content_type="application/json"
    )
    move_blob = move_bucket.blob(f"{variant}_{year_month}_{file_suffix}.csv")
    move_blob.upload_from_string(
        buffer.getvalue().encode("utf-8"), content_type="text/csv"
    )

    # Delete the blob
    if not is_local:
        blob.delete()
    print("Done with", event_name)


# If we are in the cloud function (not running locally), just define the function but don't call it
if not os.environ.get("FUNCTION_TARGET") and not os.environ.get("FUNCTION_NAME"):
    process_pgn(
        {
            "name": "lichess_db_racingKings_rated_2023-01_0001.pgn",
            "isLocal": True,
        },
        None,
    )
