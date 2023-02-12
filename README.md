# lichess-bigquery

```sql
SELECT REGEXP_REPLACE(move, '[#!?]', '') as cleaned_move, case when mod(ply, 2) = 0 then 'black' else 'white' end as color, count(*) cnt FROM `greg-finley.lichess.moves_python`
where ply > 20
group by 1,2 order by 3 desc
```

## Setting up a VM to run this script

```
sudo apt-get update
sudo apt-get install git
sudo apt-get install python3-distutils
curl -sSL https://install.python-poetry.org | python3 -
echo 'export PATH="/home/gregoryfinley/.local/bin:$PATH"' >> .bashrc
sudo apt install python3 python3-dev python3-venv
sudo apt-get install zstd
```

## Potential space savings by recasting the data

```sql
SELECT * except (Variant, FEN, Site), case when Variant = 'Racing Kings' then 'RC' else Variant end as Variant, case when FEN = '8/8/8/8/8/8/krbnNBRK/qrbnNBRQ w - - 0 1' and Variant = 'Racing Kings' then null else FEN end as FEN, replace(Site, 'https://lichess.org/', '') as GameId FROM `greg-finley.lichess.games_python`
```
