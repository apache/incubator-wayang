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

import java.util.regex.Pattern

import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters}
import org.apache.wayang.core.api.Configuration

object TeraApp extends ExperimentDescriptor {

  val KEY_LEN = 10
  val VALUE_LEN = 100
  val RECORD_LEN : Int = KEY_LEN + VALUE_LEN

  override def version = "0.1.0"

  def main(args: Array[String]) {
    // Parse args.
    if (args.isEmpty) {
      println(s"Usage: " +
        s"${Parameters.experimentHelp} " +
        s"<plugin(,plugin)*> " +
        s"<task could be[generate|sort|validate]> " +
        s"<file size ([0-9]+(.[0-9]+)?)([B|k|K|m|M|g|G|t|T])> " +
        s"<partitions>" +
        s"<input file if not value is null> " +
        s"<output file if not value is null>")
      sys.exit(1)
    }
    implicit val configuration = new Configuration
    implicit val experiment = Parameters.createExperiment(args(0), this)
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val task = args(2)
    experiment.getSubject.addConfiguration("task", task)
    val fileSize = sizeStrToBytes(args(3))
    experiment.getSubject.addConfiguration("fileSize", fileSize)
    val partitions = args(4).toInt
    experiment.getSubject.addConfiguration("partitions", partitions)
    val input_file = if(args(5).equals("null")) null else args(5)
    val output_file = if(args.length > 6){ if(args(6).equals("null")) null else args(6) } else null
    experiment.getSubject.addConfiguration("inputFile", input_file)
    experiment.getSubject.addConfiguration("outputFile", output_file)

    task match {
      case "generate" => new TeraGen(plugins: _*).apply(output_file, fileSize, partitions)
      case "sort" => new TeraSort(plugins: _*).apply(input_file, output_file)
      case "validate" => new TeraValidate(plugins: _*).apply(input_file)
    }


    // Run wordCount.
//    val wordCount = new WordCountScala(plugins: _*)
//    val words =
//      (if (wordsPerLine != null) {
//        wordCount(inputFile, wordsPerLine)
//      } else {
//        wordCount(inputFile)
//      }).toSeq.sortBy(-_._2)
//
//    // Store experiment data.
//    val inputFileSize = FileSystems.getFileSize(inputFile)
//    if (inputFileSize.isPresent) experiment.getSubject.addConfiguration("inputSize", inputFileSize.getAsLong)
//    ProfileDBHelper.store(experiment, configuration)
//
//    // Print results.
//    println(s"Found ${words.size} words:")
//    words.take(10).foreach(wc => println(s"${wc._2}x ${wc._1}"))
//    if (words.size > 10) print(s"${words.size - 10} more...")


  }

  /**
   * Convert the string format ([0-9]+(.[0-9]+)?)([B|k|K|m|M|g|G|t|T]) to the
   * number on bytes
   *
   * B = Bytes
   * k|K = Kilobytes (1_024 Bytes)
   * m|M = Megabytes (1_048_576 Bytes)
   * g|G = Gigabytes (1_073_741_824 Bytes)
   * t|T = Terabytes (1_099_511_627_776 Bytes)
   *
   * @param str in the format
   * @return number equivalent to the byte
   */
  def sizeStrToBytes(str: String): Long = {
    val reg = "(\\d+(\\.\\d+)?)([B|k|K|m|M|g|G|t|T])"
    val groups = Pattern.compile(reg).matcher(str)
    groups.find()

    val number_part:Double = groups.group(1).toDouble
    val letter_part:String = groups.group(3)

    val conversion = letter_part match {
      case "B" => 1L //2^0
      case "k" => 1024L //2^10
      case "K" => 1024L //2^10
      case "m" => 1048576L //2^20
      case "M" => 1048576L //2^20
      case "g" => 1073741824L //2^30
      case "G" => 1073741824L //2^30
      case "t" => 1099511627776L //2^40
      case "T" => 1099511627776L //2^40
      case _ => 1L //2^0
    }
    (number_part * conversion).toLong
  }

  /**
   * take a number that represent a size on bytes return the human readable version
   *
   * @param size number that represent the size
   * @return human readable version of the size
   */
  def sizeToSizeStr(size: Long): String = {
    val kbScale: Long = 1024L
    val mbScale: Long = 1024L * kbScale
    val gbScale: Long = 1024L * mbScale
    val tbScale: Long = 1024L * gbScale

    if (size > tbScale) {
      size / tbScale + "TB"
    } else if (size > gbScale) {
      size / gbScale  + "GB"
    } else if (size > mbScale) {
      size / mbScale + "MB"
    } else if (size > kbScale) {
      size / kbScale + "KB"
    } else {
      size + "B"
    }
  }

}
