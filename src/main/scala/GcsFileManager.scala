package PgnParser

import com.google.cloud.storage._
import java.nio.file.{Paths, StandardOpenOption}
import java.nio.ByteBuffer

object GcsFileManager {
  def copyFileToGcs(
      bucketName: String,
      localFilePath: String,
      blobName: String
  ): Unit = {
    val storage = StorageOptions.getDefaultInstance().getService()

    val blobInfo = BlobInfo.newBuilder(bucketName, blobName).build()

    storage.createFrom(blobInfo, Paths.get(localFilePath));
  }

  def deleteGcsFile(bucketName: String, blobName: String): Unit = {
    val storage = StorageOptions.getDefaultInstance().getService()
    val blob = storage.get(bucketName, blobName)
    blob.delete()
  }
}
