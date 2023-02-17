import chess.format.pgn.{Parser, ParsedPgn, PgnStr}
import scala.collection.mutable.ListBuffer
import cats.data.Validated

@main def parsePgn: Unit = 
  val source = scala.io.Source.fromFile("lichess_db_racingKings_rated_2023-01.pgn")
  var lines = new ListBuffer[String]()
  for (line <- source.getLines()) {
    lines += line
    if (line.startsWith("1.")) {
      //println("Found a game")
      val pgn = PgnStr(lines.mkString("\n"))
      val parsedPgn = Parser.full(pgn)
      parsedPgn match
        case Validated.Invalid(errors) =>
          println(s"Failed to parse PGN: ${errors.toString()}")
          // halt the program
          sys.exit(1)
        case Validated.Valid(parsedPgn) =>
          // parsedPgn.sans.value.foreach(x => println(x.metas))
          println(parsedPgn.sans)
          println(parsedPgn.tags)
      lines = new ListBuffer[String]()
    }
  }
