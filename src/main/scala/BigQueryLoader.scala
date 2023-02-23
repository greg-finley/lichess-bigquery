package PgnParser

import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  JobInfo,
  LoadJobConfiguration,
  Schema,
  StandardSQLTypeName,
  TableId
}
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery.JobInfo.CreateDisposition

object BigQueryLoader {
  def loadCSVToBigQuery(tableId: TableId, schema: Schema, gcsPath: String) =
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance().getService()

    val formatOptions = FormatOptions.csv()

    val loadJobConfig = LoadJobConfiguration
      .newBuilder(tableId, gcsPath)
      .setSchema(schema)
      .setFormatOptions(formatOptions)
      .setWriteDisposition(WriteDisposition.WRITE_APPEND)
      .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
      .build()

    val job = bigquery.create(JobInfo.of(loadJobConfig))
    job.waitFor()

    if (job.getStatus().getError() == null) {
      println("CSV file loaded into BigQuery table successfully.")
    } else {
      println(
        "Error loading CSV file into BigQuery table: " + job
          .getStatus()
          .getError()
          .getMessage()
      )
      sys.exit(1)
    }
}
