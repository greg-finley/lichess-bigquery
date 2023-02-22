package PgnParser

import com.google.cloud.storage._
import java.nio.file.{Paths, StandardOpenOption}
import java.nio.ByteBuffer

object GcsFileUploader {
  def copyFileToGcs(
      bucketName: String,
      localFilePath: String,
      blobName: String
  ): Unit = {
    val storage = StorageOptions.getDefaultInstance().getService()

    val blobInfo = BlobInfo.newBuilder(bucketName, blobName).build()

    val buffer = ByteBuffer.allocate(4096)
    val channel = java.nio.file.Files
      .newByteChannel(Paths.get(localFilePath), StandardOpenOption.READ)

    val blob = storage.create(blobInfo)
    while (channel.read(buffer) > 0) {
      buffer.flip()
      blob.writer().write(buffer)
      buffer.clear()
    }

    channel.close()
  }

  def deleteGcsFile(bucketName: String, blobName: String): Unit = {
    val storage = StorageOptions.getDefaultInstance().getService()
    val blob = storage.get(bucketName, blobName)
    blob.delete()
  }
}
