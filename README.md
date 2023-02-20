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
- Get checkmated despite having three queens
- Highest move number to achieve a unique FEN
- Player involved in most unique FENs
- Reproduce some of the analysis from Lichess Game Insights

## Setting up a VM to run this script

```
sudo apt-get update
sudo apt-get install apt-transport-https curl gnupg -yqq
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt wget -y
wget https://download.oracle.com/java/19/latest/jdk-19_linux-x64_bin.deb
sudo apt-get -qqy install ./jdk-19_linux-x64_bin.deb
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-19/bin/java 1919
git clone https://github.com/greg-finley/lichess-bigquery
git clone https://github.com/lichess-org/scalachess.git
git clone https://github.com/ornicar/scalalib.git
cd scalalib && sbt publishLocal && cd ..
cd scalachess && git checkout a595cf1081132209e3b084ec94ec44fa882970d6 && sbt publishLocal && cd ..
cd lichess-bigquery && sbt compile
sudo apt-get install zstd -y
curl https://database.lichess.org/crazyhouse/lichess_db_crazyhouse_rated_2023-01.pgn.zst --output lichess_db_crazyhouse_rated_2023-01.pgn.zst
pzstd -d lichess_db_crazyhouse_rated_2023-01.pgn.zst
rm lichess_db_crazyhouse_rated_2023-01.pgn.zst
sbt run
bq load --noreplace --location=EU --source_format=CSV lichess.moves_crazyhouse_2023_01  moves.csv move_schema.json
bq load --noreplace --location=EU --source_format=CSV lichess.games_crazyhouse_2023_01  games.csv game_schema.json
```

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
