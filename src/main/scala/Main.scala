import chess.format.pgn.{Parser, ParsedPgn, PgnStr}
import scala.collection.mutable.ListBuffer
import cats.data.Validated



@main def parsePgn: Unit = 
      val source = scala.io.Source.fromFile("lichess_db_racingKings_rated_2023-01.pgn")
      for (line <- source.getLines()) {
        var lines = new ListBuffer[String]()
        lines += line
        if (line.startsWith("1.")) {
          //println("Found a game")
          val pgn = PgnStr(line)
          val parsedPgn = Parser.full(pgn)
          parsedPgn match
            case Validated.Invalid(errors) =>
              println(s"Failed to parse PGN: ${errors.toString()}")
            case Validated.Valid(parsedPgn) =>
              println(parsedPgn.sans)
          // println(parsedPgn)
          lines = new ListBuffer[String]()
        }
      }

  // val parsedPgn = Parser.full(pgn)
  // println(parsedPgn)
