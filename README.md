# lichess-bigquery

```sql
SELECT REGEXP_REPLACE(move, '[#!?]', '') as cleaned_move, case when mod(ply, 2) = 0 then 'black' else 'white' end as color, count(*) cnt FROM `greg-finley.lichess.moves_python`
where ply > 20
group by 1,2 order by 3 desc
```
