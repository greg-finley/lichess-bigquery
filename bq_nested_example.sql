INSERT INTO `greg-finley.playground.nested` (game_id, moves)
VALUES
  ("game1", [
    (1, "e4", "Good move"), 
    (2, "e5", "Also good"), 
    (3, "Nf3", "This is a developing move")
  ]),
  ("game2", [
    (1, "d4", "This move controls the center"),
    (2, "d5", "Another center pawn"),
    (3, "Nf3", "Knight to f3")
  ]),
  ("game3", [
    (1, "Nf3", "This is a flexible move"),
    (2, "d5", "Another center pawn"),
    (3, "c4", "The English opening")
  ]),
  ("game1", [
    (1, "e4", cast(null as string))
  ])
  ;
