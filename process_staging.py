from dataclasses import dataclass

from google.cloud import bigquery

"""
Once process_pgn has landed files into staging, we need to process them and load to prod
"""

bigquery_client = bigquery.Client()
staging_dataset_id = "lichessstaging"
prod_dataset_id = "lichess"


@dataclass
class Column:
    table_name: str
    column_name: str


def staging_table_name_to_prod_table_name(staging_table_name: str) -> str:
    # 'games_threeCheck_2014-09_0007' -> 'games_threeCheck_2014-09'
    return "_".join(staging_table_name.split("_")[:-1])


def process_variant_month(columns: list[Column]):
    staging_games_table_names: set[str] = set()
    staging_moves_table_names: set[str] = set()
    game_column_names: set[str] = set()

    for column in columns:
        if column.table_name.startswith("games"):
            staging_games_table_names.add(column.table_name)
            game_column_names.add(column.column_name)
        elif column.table_name.startswith("moves"):
            staging_moves_table_names.add(column.table_name)
        else:
            raise ValueError(f"Unexpected table name: {column.table_name}")

    print(staging_games_table_names)
    print(staging_moves_table_names)
    print(game_column_names)

    if staging_games_table_names:
        table_list = list(staging_games_table_names)
        query = f"""
        CREATE TABLE `{prod_dataset_id}.{staging_table_name_to_prod_table_name(table_list[0])}` AS
        SELECT * EXCEPT (row_number)
        FROM (
        Select *, ROW_NUMBER() OVER (PARTITION BY GameId) AS row_number
        FROM (
        """
        for i, table_name in enumerate(table_list):
            column_selects = []
            existing_columns_for_this_table = [
                column.column_name
                for column in columns
                if column.table_name == table_name
            ]

            for column_name in game_column_names:
                if column_name in existing_columns_for_this_table:
                    column_selects.append(column_name)
                else:
                    column_selects.append("CAST(NULL AS STRING) AS " + column_name)

            query += f"""
            SELECT {", ".join(column_selects)}
            FROM `{staging_dataset_id}.{table_name}`
            {"UNION ALL" if i < len(table_list) - 1 else ""}
            """

        query += """
        )

        )
        WHERE row_number = 1
        """
        print(query)
        bigquery_client.query(query).result()

        for table_name in staging_games_table_names:
            print(f"Deleting {table_name}")
            bigquery_client.delete_table(
                f"{staging_dataset_id}.{table_name}", not_found_ok=True
            )

    if staging_moves_table_names:
        # TODO
        pass


# Get columns from staging
information_schema_columns = bigquery_client.query(
    f"""
    select table_name, column_name
    from `{staging_dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    """
).result()

columns = [Column(**row) for row in information_schema_columns]
# Segment the columns together by game/moves, variant and month. i.e lichessstaging.games_threeCheck_2014-08_0017 goes with other lichessstaging.games_threeCheck_2014
variant_months = {}
for column in columns:
    table_name_split = column.table_name.split("_")
    variant = table_name_split[1]
    month = table_name_split[2]
    variant_month = f"{variant}_{month}"
    if variant_month not in variant_months:
        variant_months[variant_month] = []
    variant_months[variant_month].append(column)

for variant_month in variant_months.values():
    process_variant_month(variant_month)

print(variant_months.keys())
