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


package org.apache.wayang.multicontext.apps.tpch

import org.apache.wayang.api.{MultiContext, MultiContextPlanBuilder}
import org.apache.wayang.apps.tpch.CsvUtils
import org.apache.wayang.apps.tpch.data.LineItem
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark
import org.apache.wayang.apps.tpch.queries.{Query1 => Query1Utils}
import org.apache.wayang.multicontext.apps.loadConfig

class Query1 {}

object Query1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2 && args.length != 4) {
      println("Usage: <lineitem-url> <delta> <optional-path-to-config-1> <optional-path-to-config-2>")
      System.exit(1)
    }

    println("TPC-H querying #1 in multi context wayang!")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args.drop(2))

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new MultiContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2)).withUdfJarsOf(classOf[Query1])

    // Example structure of lineitem file:
    // 1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the
    // 1|67310|7311|2|36|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold
    // ...
    val lineItemFile = args(0)
    val delta = args(1).toInt

    multiContextPlanBuilder

      // Load lineitem file
      .forEach(_.readTextFile(lineItemFile))

      // Parse
      .forEach(_.map(s => LineItem.parseCsv(s)))

      // Filter line items
      .forEach(_.filter(t => t.shipDate <= CsvUtils.parseDate("1998-12-01") - delta))

      // Project line items
      .forEach(_.map(t => (t.returnFlag, t.lineStatus, t.quantity, t.extendedPrice, t.discount, t.tax)))

      // Calculate result fields
      .forEach(_.map { case (returnFlag, lineStatus, quantity, extendedPrice, discount, tax) =>
        Query1Utils.Result(
          returnFlag.toString,
          lineStatus.toString,
          quantity,
          extendedPrice,
          extendedPrice * (1 - discount),
          extendedPrice * (1 - discount) * (1 + tax),
          quantity,
          extendedPrice,
          discount,
          1
        )
      })

      // Aggregate line items
      .forEach(_.reduceByKey(
        result => (result.l_returnflag, result.l_linestatus),
        (r1, r2) => Query1Utils.Result(
          r1.l_returnflag,
          r1.l_linestatus,
          r1.sum_qty + r2.sum_qty,
          r1.sum_base_price + r2.sum_base_price,
          r1.sum_disc_price + r2.sum_disc_price,
          r1.sum_charge + r2.sum_charge,
          r1.avg_qty + r2.avg_qty,
          r1.avg_price + r2.avg_price,
          r1.avg_disc + r2.avg_disc,
          r1.count_order + r2.count_order
        )
      ))

      // Post-process line items aggregates
      .forEach(_.map(result => Query1Utils.Result(
        result.l_returnflag,
        result.l_linestatus,
        result.sum_qty,
        result.sum_base_price,
        result.sum_disc_price,
        result.sum_charge,
        result.avg_qty / result.count_order,
        result.avg_price / result.count_order,
        result.avg_disc / result.count_order,
        result.count_order
      )))

      // Execute
      .execute()


  }
}
