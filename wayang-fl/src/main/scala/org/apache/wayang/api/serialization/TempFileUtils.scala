/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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