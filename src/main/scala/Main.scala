import chess.format.pgn.{Parser, ParsedPgn, PgnStr}

@main def parsePgn: Unit = 
  val pgn = PgnStr(scala.io.Source.fromFile("lichess_db_racingKings_rated_2023-01_0001.pgn").mkString)
  val parsedPgn = Parser.full(pgn)
  println(parsedPgn)
