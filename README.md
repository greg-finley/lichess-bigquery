# lichess-bigquery

## What

This library exposes the [Lichess database](https://database.lichess.org/) as public Google BigQuery tables that anyone can query.

## Why

1. Dive straight into the data without having to parse or store it yourself. The history of Standard lichess games is about 7 TB uncompressed.
2. Extract subsets of games (your games, tournament games, grandmasters whose handles start with the letter "d") without having to parse irrelevant games.
3. Do fun stuff with SQL! (EXPAND WITH EXAMPLES)

## TODO

1. Explain project
2. Tell about how to sign up for BigQuery and give samples, how to find all available tables, how it's free up to certain limits, example how to use partitions
3. Ask Google Cloud sales about storage billing model https://cloud.google.com/bigquery/docs/updating-datasets#update_storage_billing_models
4. Handle all the giant files for standard chess
5. Section encouraging people to tell me if they did anything interesting with the data

## Sample queries

(TO COME)

- Has someone put a knight on all four corners of the board at some point in a game and still won?
- Reproduce some of the analysis from Lichess Game Insights

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

`nohup python3 -u main.py &`

## Games without moves

```sql
with moves as (SELECT game_id FROM `greg-finley.lichess.moves_antichess_2014_12`
group by 1)
select g.* from `greg-finley.lichess.games_antichess_2014_12` g
left join moves
on moves.game_id = g.GameId
where moves.game_id is null
limit 100
```
