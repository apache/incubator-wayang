package org.apache.wayang.api.serialization

import java.io.{FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.reflect.ClassTag

object TempFileUtils {

  def writeToTempFileAsBinary(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    val fos = new FileOutputStream(tempFile.toFile)
    try {
      fos.write(SerializationUtils.serialize(obj))
    } finally {
      fos.close()
    }
    tempFile
  }


  def readFromTempFileFromBinary[T : ClassTag](path: Path): T = {
    val fis = new FileInputStream(path.toFile)
    try {
      SerializationUtils.deserialize[T](fis.readAllBytes())
    } finally {
      fis.close()
      Files.deleteIfExists(path)
    }
  }


  def writeToTempFileAsString(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    val serializedString = SerializationUtils.serializeAsString(obj)
    Files.writeString(tempFile, serializedString, StandardCharsets.UTF_8)
    tempFile
  }


  def readFromTempFileFromString[T: ClassTag](path: Path): T = {
    val serializedString = Files.readString(path, StandardCharsets.UTF_8)
    val deserializedObject = SerializationUtils.deserializeFromString[T](serializedString)
    Files.deleteIfExists(path)
    deserializedObject
  }
}