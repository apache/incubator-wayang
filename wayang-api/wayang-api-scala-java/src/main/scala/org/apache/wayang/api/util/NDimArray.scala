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
package org.apache.wayang.api.util

import scala.reflect.ClassTag

class NDimArray(val origin: String, val depth: Integer) extends Serializable {

  def createArrayType(baseType: Class[_], depth: Int): Class[_] = {
    if (depth == 0) baseType
    else {
      var arrayClass = baseType
      for (_ <- 1 to depth) {
        arrayClass = java.lang.reflect.Array.newInstance(arrayClass, 0).getClass
      }
      arrayClass
    }
  }

  def toClassTag(): Class[_ <: Any] = {
    val baseType = this.origin match {
      case "Integer" => classOf[Int]
      case "Boolean" => classOf[Boolean]
      case "Float" => classOf[Float]
      case "Double" => classOf[Double]
      case "Long" => classOf[Long]
      case "Character" => classOf[Char]
      case "Byte" => classOf[Byte]
      case "Short" => classOf[Short]
      case _ => classOf[Object]
    }

    createArrayType(baseType, this.depth)
  }

}

