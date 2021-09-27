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

package org.apache.wayang.apps.terasort

import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin

class TeraGen(@transient plugins: Plugin*) extends Serializable {

  def apply(output_url: String, file_size: Long, partitions: Int)
           (implicit configuration: Configuration, experiment: Experiment) = {
    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)

    val parts = partitions
    val recordsPerPartition = file_size / TeraApp.VALUE_LEN / parts.toLong
    val numRecords = recordsPerPartition * parts.toLong

    assert(recordsPerPartition < Int.MaxValue, s"records per partition > ${Int.MaxValue}")

    println("===========================================================================")
    println("===========================================================================")
    println(s"Input size: $file_size")
    println(s"Total number of records: $numRecords")
    println(s"Number of output partitions: $parts")
    println("Number of records/output partition: " + (numRecords / parts))
    println("===========================================================================")
    println("===========================================================================")

    planBuilder
      .withJobName(s"Terasort generate ${file_size}")
      .withExperiment(experiment)
      .withUdfJarsOf(this.getClass)
      .loadCollection(1 to parts)
      .flatMap( index => {
        val one = new Unsigned16(1)
        val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition.toLong)
        val recordsToGenerate = new Unsigned16(recordsPerPartition)

        val recordNumber = new Unsigned16(firstRecordNumber)
        val lastRecordNumber = new Unsigned16(firstRecordNumber)
        lastRecordNumber.add(recordsToGenerate)

        val rand = Random16.skipAhead(firstRecordNumber)

        Iterator.tabulate(recordsPerPartition.toInt) { offset =>
          val rowBytes: Array[Byte] = new Array[Byte](TeraApp.RECORD_LEN)
          val key = new Array[Byte](TeraApp.KEY_LEN)
          val value = new Array[Byte](TeraApp.VALUE_LEN)
          Random16.nextRand(rand)
          generateRecord(rowBytes, rand, recordNumber)
          recordNumber.add(one)
          rowBytes.copyToArray(key, 0, TeraApp.KEY_LEN)
          rowBytes.takeRight(TeraApp.VALUE_LEN).copyToArray(value, 0, TeraApp.VALUE_LEN)
          (key, value)
        }.toStream
      })
      .writeObjectFile(output_url)
  }

  /**
   * Generate a binary record suitable for all sort benchmarks except PennySort.
   *
   * @param recBuf record to return
   */
  def generateRecord(recBuf: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      recBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    recBuf(10) = 0x00.toByte
    recBuf(11) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record.
    i = 0
    while (i < 32) {
      recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(44) = 0x88.toByte
    recBuf(45) = 0x99.toByte
    recBuf(46) = 0xAA.toByte
    recBuf(47) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      recBuf(48 + i * 4) = v
      recBuf(49 + i * 4) = v
      recBuf(50 + i * 4) = v
      recBuf(51 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(96) = 0xCC.toByte
    recBuf(97) = 0xDD.toByte
    recBuf(98) = 0xEE.toByte
    recBuf(99) = 0xFF.toByte
  }

}
