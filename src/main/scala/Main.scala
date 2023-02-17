import chess.format.pgn.{Parser, ParsedPgn, PgnStr, Tag}
import scala.collection.mutable.ListBuffer
import cats.data.Validated
import scala.util.control.Breaks._


@main def parsePgn: Unit = 
  val source = scala.io.Source.fromFile("lichess_db_racingKings_rated_2023-01.pgn")
  val lines = new ListBuffer[String]()
  var count = 0
  breakable {
  for (line <- source.getLines()) {
    lines += line
    if (line.startsWith("1.")) {
      //println("Found a game")
      count += 1
      val pgn = PgnStr(lines.mkString("\n"))
      Parser.full(pgn).fold(errors => {
          println(s"Failed to parse PGN: ${errors.toString()}")
          // halt the program
          sys.exit(1)},
        parsedPgn => {
          // parsedPgn.sans.value.foreach(x => println(x.metas))
          parsedPgn.sans.value.foreach(x => println(x.metas))
          println(parsedPgn.tags)
          println(parsedPgn.initialPosition)
        })
      lines.clear()
      break
    }
  }}
  println(s"Found $count games")
  println(Tag.tagTypes)
