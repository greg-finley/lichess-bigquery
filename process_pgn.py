from __future__ import annotations

import os
from io import TextIOWrapper
from typing import Any

import chess.pgn
from chess.engine import Cp, Mate, PovScore
from google.cloud import bigquery, bigquery_storage_v1, storage
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2

import moves_pb2

"""
A cloud function to process a PGN file and load it to BigQuery staging area.

This file is ok to run locally but it will hit a different cloud bucket and not delete the file.
"""


Data = list[dict[str, Any]]
project_id = "greg-finley"
dataset_id = "lichessstaging"
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


def create_moves_table(table_id: str):
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
    return create_bigquery_table(schema, table_id)


def create_games_table(game_header_keys: set[str], table_id: str):
    # Create the BigQuery schema
    schema = [
        bigquery.SchemaField(k, "STRING", mode="NULLABLE") for k in game_header_keys
    ]
    return create_bigquery_table(schema, table_id)


def bq_streaming_insert(table: bigquery.Table, data: Data):
    """Inserts data into BigQuery or raises an exception"""
    errors = bigquery_client.insert_rows(table, data)
    if errors:
        raise Exception(errors)


def bq_storage_write(
    append_rows_stream: writer.AppendRowsStream, proto_rows: types.ProtoRows
):
    request = types.AppendRowsRequest()
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows
    request.proto_rows = proto_data

    append_rows_stream.send(request)


def get_bq_append_rows_stream(moves_table: bigquery.Table) -> writer.AppendRowsStream:
    """
    Since the moves table is a known schema (as opposed to games which can have varying headers),
    we can use the BigQuery Storage API to stream the data into BigQuery.
    It's annoying to set up but it's cheaper than streaming inserts.
    """
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, moves_table.table_id)
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.COMMITTED
    write_stream = write_client.create_write_stream(
        parent=parent, write_stream=write_stream
    )
    stream_name = write_stream.name
    # Create a template with fields needed for the first request.
    request_template = types.AppendRowsRequest()

    # The initial request must contain the stream name.
    request_template.write_stream = stream_name

    # So that BigQuery knows how to parse the serialized_rows, generate a
    # protocol buffer representation of your message descriptor.
    proto_schema = types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    moves_pb2.Move.DESCRIPTOR.CopyToProto(proto_descriptor)  # type: ignore
    proto_schema.proto_descriptor = proto_descriptor
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data

    # Some stream types support an unbounded number of requests. Construct an
    # AppendRowsStream to send an arbitrary number of requests to a stream.
    return writer.AppendRowsStream(write_client, request_template)


def create_bigquery_table(
    schema: list[bigquery.SchemaField],
    table_id: str,
):
    # Create the BigQuery table if it doesn't already exist
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    print(f"Creating table {table_id}")
    return bigquery_client.create_table(table, exists_ok=True)


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
    moves = types.ProtoRows()
    bq_name = f"{variant}_{year_month}"
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

    moves_table = create_moves_table(table_id=f"moves_{bq_name}")
    games_table = create_games_table(
        game_header_keys, table_id=f"games_{bq_name}_{file_suffix}"
    )

    append_rows_stream = get_bq_append_rows_stream(moves_table)

    with blob.open("rt") as pgn:
        assert isinstance(pgn, TextIOWrapper)
        while True:
            game = chess.pgn.read_game(pgn)
            if not game:
                break

            num_games += 1
            if num_games % 50 == 0:
                print(f"Inserting moves after game {num_games}")
                bq_storage_write(append_rows_stream, moves)
                moves = types.ProtoRows()
            if num_games % 100 == 0:
                print(f"Inserting games after game {num_games}")
                bq_streaming_insert(games_table, games)
                games = []
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

                moves.serialized_rows.append(
                    moves_pb2.Move(  # type: ignore
                        game_id=game_dict["GameId"],
                        ply=ply,
                        move=ply_to_move(ply),
                        color=ply_to_color(ply),
                        san=node.san(),
                        uci=node.uci(),
                        clock=node.clock(),
                        eval=clean_eval(node.eval()),
                        fen=board.fen(),
                        shredder_fen=board.shredder_fen(),
                    ).SerializeToString()
                )

    # Insert the remaining rows
    bq_storage_write(append_rows_stream, moves)
    bq_streaming_insert(games_table, games)

    # Delete the blob
    blob.delete()
    print("Done with", event_name)


# If we are in the cloud function (not running locally), just define the function but don't call it
if not os.environ.get("FUNCTION_TARGET") and not os.environ.get("FUNCTION_NAME"):
    process_pgn(
        {"name": "lichess_db_threeCheck_rated_2015-01_0001.pgn", "isLocal": True}, None
    )
