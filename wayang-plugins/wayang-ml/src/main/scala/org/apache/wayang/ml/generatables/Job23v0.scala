
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

 package org.apache.wayang.ml.generatables

 import org.apache.wayang.api._
 import org.apache.wayang.core.api.{Configuration, WayangContext}
 import org.apache.wayang.core.plugin.Plugin
 import org.apache.wayang.apps.tpch.data.{Customer, Order, LineItem, Nation, Part, PartSupplier, Supplier}
 import org.apache.wayang.core.plugin.Plugin
 import java.sql.Date
 import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
 import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
 import org.apache.wayang.apps.tpch.CsvUtils
 import org.apache.wayang.ml.training.GeneratableJob
 import org.apache.wayang.core.function.UDFComplexity

 class Job23v0 extends GeneratableJob {

def buildPlan(args: Array[String]): DataQuanta[_] = {

 val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
 val plugins = Parameters.loadPlugins(args(0))
 val datapath = args(1)

 implicit val configuration = new Configuration
 val wayangCtx = new WayangContext(configuration)
 plugins.foreach(wayangCtx.register)
 val planBuilder = new PlanBuilder(wayangCtx)
 .withJobName(s"TPC-H (${this.getClass.getSimpleName})")

   
 val source0 = planBuilder
 .readTextFile(datapath + "lineitem.tbl")
 .withName("Read LineItem")
 .map(LineItem.parseCsv)
 .map(LineItem.toTuple)
 .withName("Parse LineItem to tuple")
    

val map1 = source0.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

val reduce3 = map1.reduce((x1, x2) => x1)
      

val map4 = reduce3.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                

 val source7 = planBuilder
 .readTextFile(datapath + "partsupp.tbl")
 .withName("Read PartSupplier")
 .map(PartSupplier.parseCsv)
 .map(PartSupplier.toTuple)
 .withName("Parse PartSupplier to tuple")
    

val map6 = source7.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.SUPERQUADRATIC)
       

val join5 = map4.keyBy[Long](_._2).join(map6.keyBy[Long](_._1)).map(x => x.field1)
       

val map8 = join5.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

 return map8.collectNoExecute()
      

 }
}
  