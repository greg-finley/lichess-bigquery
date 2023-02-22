import cats.data.Validated
import cats.Show.Shown
import chess.{Game, Pos, Ply, Situation}
import chess.format.{Fen, Uci}
import chess.format.pgn.{ParsedPgn, Parser, PgnStr, Reader}
import chess.MoveOrDrop.*

import com.google.cloud.bigquery.BigQueryOptions
import sttp.client3._

import java.time.LocalDateTime
import java.io._
import collection.convert.ImplicitConversionsToScala.*

import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.util.control.Breaks.*
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import sys.process._

// TODO: Make a proper enum
val variants = List("racingKings")

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
  val existingBigQueryTablesFuture = getExistingBigQueryTables()
  for (variant <- variants) {
    getLichessFileList(variant)
      .map(_.split("\n"))
      .map(x =>
        VariantMonthYear(
          variant,
          x(0).split("_")(3)
        )
      )
      .map(variantMonthYear => {
        val existingBigQueryTables =
          Await.result(existingBigQueryTablesFuture, Duration.Inf)
        if (
          !existingBigQueryTables
            .contains(variantMonthYear)
        ) {
          downloadAndUnzipZstFile(variantMonthYear)
          parseFile(variantMonthYear)
          writeToBigQuery(variantMonthYear)
          deletePgnFile(variantMonthYear)
        }
      })
  }

def parseFile(variantMonthYear: VariantMonthYear): Unit =
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

def getExistingBigQueryTables(): Future[Set[VariantMonthYear]] = {
  Future(
    BigQueryOptions.getDefaultInstance.getService
      .getDataset("lichess")
      .list()
      .iterateAll()
      .map(_.getTableId.getTable)
      .map(_.split("_"))
      .map(arr => VariantMonthYear(arr(1), arr(2) + "-" + arr(3)))
      .toSet
  )
}

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

def getLichessFileList(variant: String) =
  val request = basicRequest.get(
    uri"https://database.lichess.org/${variant}/list.txt"
  )

  val backend = HttpClientFutureBackend()
  request
    .send(backend)
    .map(response =>
      response.body match {
        case Left(error) =>
          println(s"Error: $error")
          sys.exit(1)
        case Right(body) => body
      }
    )

def downloadAndUnzipZstFile(variantMonthYear: VariantMonthYear) =
  // TODO: Download Standard variant from torrent instead
  val zstName =
    s"lichess_db_${variantMonthYear.variant}_rated_${variantMonthYear.monthYear}.pgn.zst"
  val curlExitCode =
    s"curl https://database.lichess.org/${variantMonthYear.variant}/${zstName} --output ${zstName}".!
  if (curlExitCode != 0) {
    println(s"Failed to download ${variantMonthYear}")
    sys.exit(1)
  }
  val unzipExitCode =
    s"pzstd -d ${zstName}".!
  if (unzipExitCode != 0) {
    println(s"Failed to unzip ${variantMonthYear}")
    sys.exit(1)
  }
  val rmExitCode =
    s"rm ${zstName}".!
  if (rmExitCode != 0) {
    println(s"Failed to remove ZST ${variantMonthYear}")
    sys.exit(1)
  }

def writeToBigQuery(variantMonthYear: VariantMonthYear) =
  // TODO: Do these in parallel
  val tableNameSuffix =
    s"_${variantMonthYear.variant}_${variantMonthYear.monthYear.replace("-", "_")}"
  val bqMovesExitCode =
    s"bq load --noreplace --location=EU --source_format=CSV lichess.moves${tableNameSuffix} moves.csv move_schema.json".!
  if (bqMovesExitCode != 0) {
    println(s"Failed to load moves to BigQuery ${variantMonthYear}")
    sys.exit(1)
  }
  val bqGamesExitCode =
    s"bq load --noreplace --location=EU --source_format=CSV lichess.games${tableNameSuffix} games.csv game_schema.json".!
  if (bqGamesExitCode != 0) {
    println(s"Failed to load games to BigQuery ${variantMonthYear}")
    sys.exit(1)
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
