import chess.format.pgn.{ParsedPgn, Parser, PgnStr, Reader}
import chess.format.Fen
import chess.format.Uci
import chess.Ply
import chess.{Game, Pos}

import scala.collection.mutable.{ListBuffer, LinkedHashMap}
import cats.data.Validated
import java.time.LocalDateTime
import java.io._

import scala.util.control.Breaks.*
import chess.Situation

// Freeze the list and ignore any future tags, so BigQuery has a consistent schema
// Maybe if we get a new tag in the future we can edit the old schemas
val allTagValues: LinkedHashMap[String, String] = LinkedHashMap(
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
    scala.io.Source.fromFile("lichess_db_crazyhouse_rated_2023-01.pgn")
  val lines: ListBuffer[String] = ListBuffer()
  val gamesFile = new File("games.csv")
  if (gamesFile.exists()) gamesFile.delete()
  val gameWriter = new PrintWriter(new FileWriter(gamesFile, true))
  var gameCount = 0
  var gameTagValues = allTagValues.clone()
  breakable {
    for (line <- source.getLines()) {
      lines += line
      if (line.startsWith("[")) then
        // [Site "https://lichess.org/CyEpEADM"]
        // tagName = Site
        // tagValue = https://lichess.org/CyEpEADM
        val tag = line.split(" ")
        val tagName = tag(0).replace("[", "")
        if (gameTagValues.contains(tagName)) then
          gameTagValues(tagName) = tag(1).replace("]", "").replaceAll("\"", "")
      else if (line.startsWith("1.")) then
        // println("Found a game")
        gameTagValues("GameId") = gameTagValues("Site").split("/")(3)
        val gameTags = gameTagValues.toSeq
          .map { case (_, value) =>
            Option(value).getOrElse("")
          }
          .foldLeft("") { (acc, value) =>
            if (acc.isEmpty) value else acc + "," + value
          }
        gameWriter.println(gameTags)

        // println(gameTagValues)
        // println(gameTags)
        // println(parseSans(line))
        gameTagValues = allTagValues.clone()
        gameCount += 1
        val pgn = PgnStr(lines.mkString("\n"))
        // Parser
        //   .full(pgn)
        //   .fold(
        //     errors => {
        //       println(s"Failed to parse PGN: ${errors.toString()}")
        //       // halt the program
        //       sys.exit(1)
        //     },
        //     parsedPgn => {
        //       // parsedPgn.sans.value.foreach(x => println(x.metas))
        //       // parsedPgn.sans.value.foreach(x => println(x.metas))
        //       // println(parsedPgn.tags)
        //       // println(parsedPgn.initialPosition)

        //       // val fen2 = Fen.Epd("8/8/8/8/8/8/krbnNBRK/qrbnNBRQ w - - 0 1")

        //       // val game: Game = Game(
        //       //   variantOption = Some(chess.variant.RacingKings),
        //       //   fen = Some(fen2)
        //       // )
        //       // println(game.situation)
        //       // println(game.situation.board)
        //       // game.apply("e2e4")
        //       // parsedPgn.tags.value.foreach(x =>
        //       //   if (tagTypes.contains(x.name)) { println(x.name) }
        //       // )

        //     }
        //   )
        val readerOutput = Reader.full(pgn)
        readerOutput.fold(
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
                replay.chronoMoves.zipWithIndex.foreach((x, index) =>
                  x.fold(
                    { y =>
                      val fullMoveNumber = Ply(index).fullMoveNumber
                      // println("fullMoveNumber: " + fullMoveNumber)
                      // println(Ply(index).color)
                      val situationAndFullMoveNumber =
                        Situation.AndFullMoveNumber(
                          y.situationAfter,
                          fullMoveNumber
                        )
                      // println(Fen.write(situationAndFullMoveNumber))
                      // println(y.toUci.uci)
                      // println(index)
                    },
                    { z =>
                      val fullMoveNumber = Ply(index).fullMoveNumber
                      // println("fullMoveNumber: " + fullMoveNumber)
                      // println(Ply(index).color)
                      val situationAndFullMoveNumber =
                        Situation.AndFullMoveNumber(
                          z.situationAfter,
                          fullMoveNumber
                        )
                      // println(Fen.write(situationAndFullMoveNumber))
                      // println(z.toUci.uci)
                      // println(index)
                    }
                  )
                )
              }
            )
          }
        )
        lines.clear()
        if (gameCount % 500 == 0) {
          println(LocalDateTime.now())
          println(gameCount)
        }
        // break
    }
  }

  gameWriter.close()
  // println(s"Found $count games")
  // println(Tag.tagTypes)

val SCORES = List("1-0\n", "0-1\n", "1/2-1/2\n")

type Move = (String, String, String)

def parseSans(game_str: String): List[Move] = {
  var moves: List[Move] = List()
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
