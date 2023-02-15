import datetime
from typing import Any

import chess.pgn
from chess.engine import Cp, Mate, PovScore
from google.cloud import bigquery, storage

"""A cloud function to process a PGN file and load it to BigQuery staging area"""

Data = list[dict[str, Any]]
dataset_id = "lichess_staging"
storage_client = storage.Client()
bigquery_client = bigquery.Client()


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


def load_moves(moves: Data, table_id: str):
    # Create the BigQuery schema
    schema = [
        bigquery.SchemaField("game_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ply", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("move", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("san", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uci", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("clock", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("eval", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fen", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("shredder_fen", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
    ]
    load_to_bigquery(moves, schema, table_id, "moves")


def load_games(games: Data, table_id: str):
    games_keys: set[str] = set()
    for game in games:
        for k in game:
            games_keys.add(k)

    # Create the BigQuery schema
    schema = [bigquery.SchemaField(k, "STRING", mode="NULLABLE") for k in games_keys]
    load_to_bigquery(games, schema, table_id, "games")


def load_to_bigquery(
    items: Data,
    schema: list[bigquery.SchemaField],
    table_id: str,
    name: str,
):
    # Create the BigQuery table if it doesn't already exist
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table, exists_ok=True)

    # Load the moves into BigQuery
    errors = bigquery_client.insert_rows_json(table, items)
    if errors:
        print(errors)
        raise Exception(f"Failed to load {name} to BigQuery")


def process_pgn(event, context):
    bucket = storage_client.bucket("lichess-bigquery-pgn")
    event_name = event["name"]
    blob = bucket.blob(event_name)

    # event["name"] = "lichess_db_standard_rated_2023-01_0001.pgn"
    variant = event_name.split("_")[2]
    year_month = event_name.split("_")[4]
    file_suffix = event_name.split("_")[5].split(".")[0]

    # We need to collect the header keys for all games and tell BigQuery to
    # make the table with these columns all as strings
    game_header_keys: set[str] = set()
    game_header_keys.add("GameId")
    num_games = 0
    games: Data = []
    moves: Data = []

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

    print("len(games)", len(games))
    print("len(moves)", len(moves))

    bq_name_suffix = f"{variant}_{year_month}_{file_suffix}"

    load_moves(moves, table_id=f"moves_{bq_name_suffix}")
    load_games(games, table_id=f"games_{bq_name_suffix}")

    # Delete the blob
    blob.delete()
    print("Done with", event_name)
