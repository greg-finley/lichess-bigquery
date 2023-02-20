import cats.data.Validated
import cats.Show.Shown
import chess.{Game, Pos, Ply, Situation}
import chess.format.{Fen, Uci}
import chess.format.pgn.{ParsedPgn, Parser, PgnStr, Reader}

import java.time.LocalDateTime
import java.io._

import scala.collection.mutable.{LinkedHashMap, ListBuffer}

import scala.util.control.Breaks.*

// Freeze the list and ignore any future tags, so BigQuery has a consistent schema
// Maybe if we get a new tag in the future we can edit the old schemas
val gameTagValues: LinkedHashMap[String, String] = LinkedHashMap(
  "Event" -> null,
  "Site" -> null,
  "GameId" -> null,
  "Date" -> null,
  "UTCDate" -> null,
  "UTCTime" -> null,
  "Round" -> null,
  "Board" -> null,
  "White" -> null,
  "Black" -> null,
  "TimeControl" -> null,
  "WhiteClock" -> null,
  "BlackClock" -> null,
  "WhiteElo" -> null,
  "BlackElo" -> null,
  "WhiteRatingDiff" -> null,
  "BlackRatingDiff" -> null,
  "WhiteTitle" -> null,
  "BlackTitle" -> null,
  "WhiteTeam" -> null,
  "BlackTeam" -> null,
  "WhiteFideId" -> null,
  "BlackFideId" -> null,
  "Result" -> null,
  "FEN" -> null,
  "Variant" -> null,
  "ECO" -> null,
  "Opening" -> null,
  "Termination" -> null,
  "Annotator" -> null
)

@main def parsePgn: Unit =
  val source =
    scala.io.Source.fromFile("lichess_db_racingKings_rated_2023-01_0001.pgn")
  val lines: ListBuffer[String] = ListBuffer()
  val gamesFile = new File("games.csv")
  val movesFile = new File("moves.csv")
  if (gamesFile.exists()) gamesFile.delete()
  if (movesFile.exists()) movesFile.delete()
  val gameWriter = new PrintWriter(new FileWriter(gamesFile, true))
  val moveWriter = new PrintWriter(new FileWriter(movesFile, true))
  var gameCount = 0
  for (line <- source.getLines()) {
    lines += line
    // tuple of (pgn move, clock, eval)
    if (line.startsWith("[")) then
      // [Site "https://lichess.org/CyEpEADM"]
      val tag = line.split(" ")
      val tagName = tag(0).replace("[", "")
      if (gameTagValues.contains(tagName)) then
        gameTagValues(tagName) = line
          .substring(tagName.length() + 2)
          .replace("]", "")
          .replaceAll("\"", "")
    else if (line.startsWith("1.")) then
      val customMoveParsing = customParseMoves(line)
      gameTagValues("GameId") = gameTagValues("Site").split("/")(3)
      gameWriter.println(
        gameTagValues.toSeq
          .map { case (_, value) =>
            Option(value).getOrElse("")
          }
          .foldLeft("") { (acc, value) =>
            if (acc.isEmpty) value else acc + "," + value
          }
      )

      gameCount += 1
      val pgn = PgnStr(lines.mkString("\n"))

      // tuple of (ply number, uci move, fen)
      val lichessMoveParsing: List[(Int, String, String, String)] = Reader
        .full(pgn)
        .fold(
          errors => {
            println(s"Failed to read: ${errors.toString()}")
            // halt the program
            sys.exit(1)
          },
          result => {
            result.valid.fold(
              errors => {
                println(s"Failed to parse PGN: ${errors.toString()}")
                // halt the program
                sys.exit(1)
              },
              replay => {
                replay.chronoMoves.zipWithIndex.map((x, index) =>
                  x.fold(
                    { move =>
                      (
                        index + 1,
                        move.toUci.uci,
                        Fen
                          .write(
                            Situation.AndFullMoveNumber(
                              move.situationAfter,
                              Ply(index + 1).fullMoveNumber
                            )
                          )
                          .toString(),
                        gameTagValues("GameId")
                      )
                    },
                    { drop =>
                      (
                        index + 1,
                        drop.toUci.uci,
                        Fen
                          .write(
                            Situation.AndFullMoveNumber(
                              drop.situationAfter,
                              Ply(index + 1).fullMoveNumber
                            )
                          )
                          .toString(),
                        gameTagValues("GameId")
                      )
                    }
                  )
                )
              }
            )
          }
        )
      // zip together custom and lichess move parsing
      customMoveParsing
        .zip(lichessMoveParsing)
        .foreach((x, y) =>
          moveWriter.println(
            (x._1, x._2, x._3, y._1, y._2, y._3, y._4).productIterator
              .mkString(",")
          )
        )
      lines.clear()
      gameTagValues.foreach((x, y) => gameTagValues(x) = null)
      if (gameCount % 500 == 0) {
        println(LocalDateTime.now())
        println(gameCount)
      }
  }

  gameWriter.close()
  moveWriter.close()

val SCORES = List("1-0\n", "0-1\n", "1/2-1/2\n")

def customParseMoves(game_str: String): List[(String, String, String)] = {
  var moves: List[(String, String, String)] = List()
  val game_split = game_str.split(" ")
  if (!game_str.contains("...")) {
    // Assume it's an old-style game, like
    // 1. Kh3 Rb4 2. Rg4 Be4 3. Kh4 0-1
    for (i <- 0 until game_split.length) {
      val part = game_split(i)
      if (part.contains(".")) {
        val move = game_split(i + 1)
        if (SCORES.contains(move)) {
          break()
        }
        var black_move = ""
        // Try to add black's move. Might not be present at the end of the game
        if (i + 2 < game_split.length) {
          black_move = game_split(i + 2)
        }
        if (!SCORES.contains(black_move)) {
          moves = moves :+ (move, "", "")
          moves = moves :+ (black_move, "", "")
        } else {
          moves = moves :+ (move, "", "")
        }
      }
    }
  } else {
    // Otherwise, assume it's a new-style game, like
    // 1. e4 { [%eval 0.17] [%clk 0:00:30] } 1... c5 { [%eval 0.19] [%clk 0:00:30] }
    for (i <- 0 until game_split.length) {
      val part = game_split(i)
      if (part.contains(".") && !part.endsWith("]")) {
        val move = game_split(i + 1)
        var clk = ""
        var eval = ""
        try {
          var j = i + 2
          while (!game_split(j).endsWith("}")) {
            if (game_split(j).startsWith("[%clk")) {
              clk = game_split(j + 1).stripSuffix("]")
            } else if (game_split(j).startsWith("[%eval")) {
              eval = game_split(j + 1).stripSuffix("]")
            }
            j += 1
          }
        } catch {
          case e: ArrayIndexOutOfBoundsException =>
        }
        moves = moves :+ (move, clk, eval)
      }
    }
  }
  return moves
}
