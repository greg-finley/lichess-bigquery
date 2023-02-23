package PgnParser

import cats.data.Validated
import cats.Show.Shown
import chess.{Game, Pos, Ply, Situation}
import chess.format.{Fen, Uci}
import chess.format.pgn.{ParsedPgn, Parser, PgnStr, Reader}
import chess.MoveOrDrop.*

import com.google.cloud.bigquery.BigQueryOptions
import sttp.client3._

import io.circe._, io.circe.parser._

import java.time.LocalDateTime
import java.io._
import collection.convert.ImplicitConversionsToScala.*

import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.util.control.Breaks.*
import scala.util.{Success, Failure}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import sys.process._

import BigQueryLoader.loadCSVToBigQuery
import GcsFileManager.{copyFileToGcs, deleteGcsFile}
import PubSubSubscriber.getSubscriber

import com.google.cloud.bigquery.{Field, Schema, TableId, StandardSQLTypeName}
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.pubsub.v1.PubsubMessage

// TODO: Make a proper enum
val variants = List(
  "antichess",
  "atomic",
  "chess960",
  "crazyhouse",
  "horde",
  "kingOfTheHill",
  "racingKings",
  // "standard",
  "threeCheck"
)

val moveSchema = Schema.of(
  Field
    .newBuilder("san", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.REQUIRED)
    .build(),
  Field
    .newBuilder("clock", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("eval", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("ply", StandardSQLTypeName.INT64)
    .setMode(Field.Mode.REQUIRED)
    .build(),
  Field
    .newBuilder("uci", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.REQUIRED)
    .build(),
  Field
    .newBuilder("fen", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.REQUIRED)
    .build(),
  Field
    .newBuilder("GameId", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.REQUIRED)
    .build()
)

val gameSchema = Schema.of(
  Field
    .newBuilder("Event", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Site", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("GameId", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Date", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("UTCDate", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("UTCTime", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Round", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Board", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("White", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Black", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("TimeControl", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("WhiteClock", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("BlackClock", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("WhiteElo", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("BlackElo", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("WhiteRatingDiff", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("BlackRatingDiff", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("WhiteTitle", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("BlackTitle", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("WhiteTeam", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("BlackTeam", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("WhiteFideId", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("BlackFideId", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Result", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("FEN", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Variant", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("ECO", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Opening", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Termination", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build(),
  Field
    .newBuilder("Annotator", StandardSQLTypeName.STRING)
    .setMode(Field.Mode.NULLABLE)
    .build()
)

case class VariantMonthYear(
    variant: String,
    monthYear: String
)

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

@main def main: Unit =
  val messageReceiver = new MessageReceiverImpl()
  val subscriber = getSubscriber(messageReceiver)
  subscriber.startAsync().awaitRunning()
  subscriber.awaitTerminated()

class MessageReceiverImpl extends MessageReceiver {
  override def receiveMessage(
      message: PubsubMessage,
      consumer: AckReplyConsumer
  ): Unit = {
    val jsonStr = message.getData.toStringUtf8
    val json = parse(jsonStr) match {
      case Left(error) =>
        throw new RuntimeException(
          s"Failed to parse message as JSON: ${error.getMessage}"
        )
      case Right(value) => value
    }
    val name = json.hcursor.downField("name").as[String] match {
      case Left(error) =>
        throw new RuntimeException(
          s"Failed to parse name from message: ${error.getMessage}"
        )
      case Right(value) => value
    }
    val bucket = json.hcursor.downField("bucket").as[String] match {
      case Left(error) =>
        throw new RuntimeException(
          s"Failed to parse bucket from message: ${error.getMessage}"
        )
      case Right(value) => value
    }
    println(
      s"Received message with ID ${message.getMessageId}: name=$name, bucket=$bucket"
    )

    consumer.ack()
  }
}

def parseFile(variantMonthYear: VariantMonthYear): Unit =
  println(s"Parsing ${variantMonthYear}")
  val source =
    scala.io.Source.fromFile(
      s"lichess_db_${variantMonthYear.variant}_rated_${variantMonthYear.monthYear}.pgn"
    )
  val lines: ListBuffer[String] = ListBuffer()
  val gamesFile =
    new File(
      "games.csv"
    ) // if parallelizing this job more, rename this to include variant and monthYear
  val movesFile = new File("moves.csv") // ditto
  if (gamesFile.exists()) gamesFile.delete()
  if (movesFile.exists()) movesFile.delete()
  val gameWriter = new PrintWriter(new FileWriter(gamesFile, true))
  val moveWriter = new PrintWriter(new FileWriter(movesFile, true))
  val futures = ListBuffer[Future[Unit]]()
  var futureCount = 0
  for (line <- source.getLines()) {
    if (line == "") then {} else if (line.startsWith("[")) then
      if (line.startsWith("[Event") && lines.length > 0) then
        // Corner case where the previous game had no moves
        val linesList = lines.toList
        futures += Future {
          processGame(linesList, "", gameWriter, moveWriter)
        }
        futureCount += 1
        lines.clear()
      lines += line
    else if (line.startsWith("1.")) then
      val linesList = lines.toList
      futures += Future {
        processGame(linesList, line, gameWriter, moveWriter)
      }
      futureCount += 1
      lines.clear()
    if (futureCount > 100) then
      for (future <- futures) {
        Await.result(future, Duration.Inf)
      }
      futures.clear()
      futureCount = 0
  }

  for (future <- futures) {
    Await.result(future, Duration.Inf)
  }

  gameWriter.close()
  moveWriter.close()

val SCORES = List("1-0\n", "0-1\n", "1/2-1/2\n")

def customParseMoves(movesStr: String): List[(String, String, String)] = {
  var moves: List[(String, String, String)] = List()
  val gameSplit = movesStr.split(" ")
  if (!movesStr.contains("...") && !movesStr.contains("{")) {
    // Assume it's an old-style game, like
    // 1. Kh3 Rb4 2. Rg4 Be4 3. Kh4 0-1
    for (i <- 0 until gameSplit.length) {
      val part = gameSplit(i)
      if (part.contains(".")) {
        val move = gameSplit(i + 1)
        if (SCORES.contains(move)) {
          break()
        }
        var black_move = ""
        // Try to add black's move. Might not be present at the end of the game
        if (i + 2 < gameSplit.length) {
          black_move = gameSplit(i + 2)
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
    for (i <- 0 until gameSplit.length) {
      val part = gameSplit(i)
      if (part.contains(".") && !part.endsWith("]")) {
        val move = gameSplit(i + 1)
        var clk = ""
        var eval = ""
        try {
          var j = i + 2
          while (!gameSplit(j).endsWith("}")) {
            if (gameSplit(j).startsWith("[%clk")) {
              clk = gameSplit(j + 1).stripSuffix("]")
            } else if (gameSplit(j).startsWith("[%eval")) {
              eval = gameSplit(j + 1).stripSuffix("]")
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

def processGame(
    headerLines: List[String],
    moveLine: String,
    gameWriter: PrintWriter,
    moveWriter: PrintWriter
): Unit = {
  val gameTagValues = allTagValues.clone()
  for (line <- headerLines) {
    if (line.startsWith("[")) {
      val tagName = line.split(" ")(0).replace("[", "")
      if (gameTagValues.contains(tagName)) then
        gameTagValues(tagName) = line
          .substring(tagName.length() + 2)
          .replace("]", "")
          .replaceAll("\"", "")
    }
  }
  val customMoveParsing = customParseMoves(moveLine)
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

  // tuple of (ply number, uci move, fen)
  val lichessMoveParsing: List[(Int, String, String, String)] = Reader
    .full(PgnStr((headerLines :+ moveLine).mkString("\n")))
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
            // Throw away the moves if invalid (some old variant games)
            List()
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
}

def writeToBigQuery(variantMonthYear: VariantMonthYear) = {
  println(s"Writing to BigQuery ${variantMonthYear}")
  val tableNameSuffix =
    s"_${variantMonthYear.variant}_${variantMonthYear.monthYear.replace("-", "_")}"

  val bucketName = "lichess-bigquery"

  val movesFuture = Future {
    GcsFileManager.copyFileToGcs(
      bucketName,
      "moves.csv",
      f"moves${tableNameSuffix}.csv"
    )
  }.map(_ =>
    BigQueryLoader.loadCSVToBigQuery(
      TableId.of("greg-finley", "lichess", s"moves${tableNameSuffix}"),
      moveSchema,
      f"gs://${bucketName}/moves${tableNameSuffix}.csv"
    )
    GcsFileManager.deleteGcsFile(
      bucketName,
      f"moves${tableNameSuffix}.csv"
    )
  )

  val gamesFuture = Future {
    GcsFileManager.copyFileToGcs(
      bucketName,
      "games.csv",
      f"games${tableNameSuffix}.csv"
    )
  }.map(_ =>
    BigQueryLoader.loadCSVToBigQuery(
      TableId.of("greg-finley", "lichess", s"games${tableNameSuffix}"),
      gameSchema,
      f"gs://${bucketName}/games${tableNameSuffix}.csv"
    )
    GcsFileManager.deleteGcsFile(
      bucketName,
      f"games${tableNameSuffix}.csv"
    )
  )

  Await.result(movesFuture, Duration.Inf)
  Await.result(gamesFuture, Duration.Inf)
}

def deletePgnFile(variantMonthYear: VariantMonthYear) =
  val pgnName =
    s"lichess_db_${variantMonthYear.variant}_rated_${variantMonthYear.monthYear}.pgn"
  val rmExitCode =
    s"rm ${pgnName}".!
  if (rmExitCode != 0) {
    println(s"Failed to remove PGN ${variantMonthYear}")
    sys.exit(1)
  }
