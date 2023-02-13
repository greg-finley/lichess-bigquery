# lichess-bigquery

## TODO

1. Explain project
2. Tell about how to sign up for BigQuery and give samples, how to find all available tables, how it's free up to certain limits, example how to use partitions
3. Decide wildcard tables vs partitioned tables https://stackoverflow.com/a/48643151/3869802 Wildcard are probably better if inconsistent schemas for games?
4. Ask Google Cloud sales about storage billing model https://cloud.google.com/bigquery/docs/updating-datasets#update_storage_billing_models
5. Attach FEN to every move?

## Sample query

```sql
SELECT REGEXP_REPLACE(move, '[#!?]', '') as cleaned_move, case when mod(ply, 2) = 0 then 'black' else 'white' end as color, count(*) cnt FROM `greg-finley.lichess.moves_python`
where ply > 20
group by 1,2 order by 3 desc
```

## Setting up a VM to run this script

```
sudo apt-get update
sudo apt-get install git python3-distutils zstd -y
sudo apt install python3 python3-dev python3-venv -y
curl -sSL https://install.python-poetry.org | python3 -
echo 'export PATH="/home/gregoryfinley/.local/bin:$PATH"' >> /home/gregoryfinley/.bashrc
source /home/gregoryfinley/.bashrc
git clone https://github.com/greg-finley/lichess-bigquery
cd lichess-bigquery && poetry install
```

## Potential space savings by recasting the data

```sql
SELECT * except (Variant, FEN, Site), case when Variant = 'Racing Kings' then 'RC' else Variant end as Variant, case when FEN = '8/8/8/8/8/8/krbnNBRK/qrbnNBRQ w - - 0 1' and Variant = 'Racing Kings' then null else FEN end as FEN, replace(Site, 'https://lichess.org/', '') as GameId FROM `greg-finley.lichess.games_python`
```
