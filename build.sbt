val scala3Version = "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "Lichess-bigquery",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.lichess" %% "scalachess" % "14.5.4"
    )
  )

lazy val parsePgn = taskKey[Unit]("Parse PGN files")
parsePgn := {
  (runMain in Compile).toTask(" ParsePgn").value
}
