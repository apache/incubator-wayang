/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.apache.wayang.plugin.hackit.api

import org.apache.wayang.api.dataquanta.DataQuantaFactory
import org.apache.wayang.api.createPlanBuilder
import org.apache.wayang.plugin.hackit.api.Hackit.underHackit
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark
import org.junit.{Assert, BeforeClass, Test}
import org.junit.jupiter.api.{BeforeAll, BeforeEach}


class ApiExtensionTest {

  @BeforeEach
  def setUp() ={
    DataQuantaFactory.setTemplate(DataQuantaHackit);
  }

  @Test
  def testReadMapCollectHackit(): Unit = {

    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Wayang plan.
    var outputValues = wayang
      .loadCollection(inputValues).withName("Load input values")
      .addTag(null)
      .map(a => a + 2)//.withName("Add 2")
      .dataQuanta
      .collect()

    print(outputValues)

    var lolo = wayang
      .loadCollection(inputValues).withName("Load input values")
      .addTag(null)
      .map(a => a + 2)//.withName("Add 2")
      .toDataQuanta()
      .collect()

    print(lolo)
//    // Check the outcome.
//    val expectedOutputValues = inputValues.map(_ + 2)
//    Assert.assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

}
