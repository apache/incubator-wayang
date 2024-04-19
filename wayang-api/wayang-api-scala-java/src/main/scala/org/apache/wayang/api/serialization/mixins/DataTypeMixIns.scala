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

package org.apache.wayang.api.serialization.mixins

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.types.{BasicDataUnitType, DataUnitGroupType, DataUnitType}

object DataTypeMixIns {


  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[BasicDataUnitType[_]], name = "BasicDataUnitType"),
    new JsonSubTypes.Type(value = classOf[DataUnitGroupType[_]], name = "DataUnitGroupType"),
  ))
  abstract class DataUnitTypeMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[RecordType], name = "RecordType"),
  ))
  abstract class BasicDataUnitTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("typeClass") typeClass: Class[T]) = {
      this()
    }
  }

  abstract class RecordTypeMixIn {
    @JsonCreator
    def this(@JsonProperty("fieldNames") fieldNames: Array[String]) = {
      this()
    }
  }

  abstract class DataUnitGroupTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("baseType") baseType: DataUnitType[_]) = {
      this()
    }
  }

  abstract class DataSetTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("dataUnitType") dataUnitType: DataUnitType[T]) = {
      this()
    }
  }

}