val scala3Version = "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "Lichess-bigquery",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.lichess" %% "scalachess" % "14.5.1",
      "com.google.cloud" % "google-cloud-bigquery" % "2.22.0",
      "com.google.cloud" % "google-cloud-pubsub" % "1.123.3",
      "com.google.cloud" % "google-cloud-storage" % "2.19.0",
      "com.softwaremill.sttp.client3" %% "core" % "3.8.11",
      "io.circe" %% "circe-parser" % "0.14.4"
    )
  )
