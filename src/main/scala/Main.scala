import chess.format.pgn.{ParsedPgn, Parser, PgnStr, Tag, Reader}
import chess.format.Fen
import chess.format.Uci
import chess.Ply
import chess.{Game, Pos}

import scala.collection.mutable.ListBuffer
import cats.data.Validated
import java.time.LocalDateTime

import scala.util.control.Breaks.*
import chess.Situation

@main def parsePgn: Unit =
  val source =
    scala.io.Source.fromFile("lichess_db_crazyhouse_rated_2023-01.pgn")
  val lines = new ListBuffer[String]()
  var count = 0
  breakable {
    for (line <- source.getLines()) {
      lines += line
      if (line.startsWith("1.")) {
        // println("Found a game")
        count += 1
        val pgn = PgnStr(lines.mkString("\n"))
        Parser
          .full(pgn)
          .fold(
            errors => {
              println(s"Failed to parse PGN: ${errors.toString()}")
              // halt the program
              sys.exit(1)
            },
            parsedPgn => {
              // parsedPgn.sans.value.foreach(x => println(x.metas))
              // parsedPgn.sans.value.foreach(x => println(x.metas))
              // println(parsedPgn.tags)
              // println(parsedPgn.initialPosition)

              // val fen2 = Fen.Epd("8/8/8/8/8/8/krbnNBRK/qrbnNBRQ w - - 0 1")

              // val game: Game = Game(
              //   variantOption = Some(chess.variant.RacingKings),
              //   fen = Some(fen2)
              // )
              // println(game.situation)
              // println(game.situation.board)
              // game.apply("e2e4")

            }
          )
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
                replay.moves.reverse.zipWithIndex.foreach((x, index) =>
                  x.fold(
                    { y =>
                      val fullMoveNumber = Ply(index).fullMoveNumber
                      println("fullMoveNumber: " + fullMoveNumber)
                      println(Ply(index).color)
                      val situationAndFullMoveNumber =
                        Situation.AndFullMoveNumber(
                          y.situationAfter,
                          fullMoveNumber
                        )
                      println(Fen.write(situationAndFullMoveNumber))
                      println(y.toUci.uci)
                      // println(index)
                    },
                    { z =>
                      val fullMoveNumber = Ply(index).fullMoveNumber
                      println("fullMoveNumber: " + fullMoveNumber)
                      println(Ply(index).color)
                      val situationAndFullMoveNumber =
                        Situation.AndFullMoveNumber(
                          z.situationAfter,
                          fullMoveNumber
                        )
                      println(Fen.write(situationAndFullMoveNumber))
                      println(z.toUci.uci)
                      // println(index)
                    }
                  )
                )
                // Translate back to FEN
                replay.state.board
              }
            )
          }
        )
        lines.clear()
        if (count % 500 == 0) {
          println(LocalDateTime.now())
          println(count)
        }
        break
      }
    }
  }
  // println(s"Found $count games")
  // println(Tag.tagTypes)
