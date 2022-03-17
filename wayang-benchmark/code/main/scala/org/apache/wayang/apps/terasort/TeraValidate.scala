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

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.util.PureJavaCrc32
import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin

class TeraValidate(@transient plugins: Plugin*) extends Serializable {

  def apply(input_url: String)
           (implicit configuration: Configuration, experiment: Experiment) = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
    val dataset = planBuilder
      .readObjectFile[Tuple2[Array[Byte], Array[Byte]]](input_url)

    val output = dataset.mapPartitions(
      iterable_element => {
        val iter = iterable_element.iterator
        val sum = new Unsigned16
        val checksum = new Unsigned16
        val crc32 = new PureJavaCrc32()
        val min = new Array[Byte](10)
        val max = new Array[Byte](10)

        val cmp = UnsignedBytes.lexicographicalComparator()

        var pos = 0L
        var prev = new Array[Byte](10)

        while (iter.hasNext) {
          val key = iter.next()._1
          assert(cmp.compare(key, prev) >= 0)

          crc32.reset()
          crc32.update(key, 0, key.length)
          checksum.set(crc32.getValue)
          sum.add(checksum)

          if (pos == 0) {
            key.copyToArray(min, 0, 10)
          }
          pos += 1
          prev = key
        }
        prev.copyToArray(max, 0, 10)
        Iterator((sum, min, max)).toStream
      }
    )

    val checksumOutput = output.collect()
    val cmp = UnsignedBytes.lexicographicalComparator()
    val sum = new Unsigned16
    var numRecords = dataset.count.collect().head

    checksumOutput.foreach { case (partSum, min, max) =>
      sum.add(partSum)
    }
    println("num records: " + numRecords)
    println("checksum: " + sum.toString)
    var lastMax = new Array[Byte](10)
    checksumOutput.map{ case (partSum, min, max) =>
      (partSum, min.clone(), max.clone())
    }.zipWithIndex.foreach { case ((partSum, min, max), i) =>
      println(s"part $i")
      println(s"lastMax" + lastMax.toSeq.map(x => if (x < 0) 256 + x else x))
      println(s"min " + min.toSeq.map(x => if (x < 0) 256 + x else x))
      println(s"max " + max.toSeq.map(x => if (x < 0) 256 + x else x))
      assert(cmp.compare(min, max) <= 0, "min >= max")
      assert(cmp.compare(lastMax, min) <= 0, "current partition min < last partition max")
      lastMax = max
    }
    println("num records: " + numRecords)
    println("checksum: " + sum.toString)
    println("partitions are properly sorted")
  }

}
