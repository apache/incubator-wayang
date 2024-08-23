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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonCreator, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate
import org.apache.wayang.core.function.{AggregationDescriptor, ConsumerDescriptor, FlatMapDescriptor, FunctionDescriptor, MapPartitionsDescriptor, PredicateDescriptor, ReduceDescriptor, TransformationDescriptor}
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator
import org.apache.wayang.core.types.{BasicDataUnitType, DataUnitGroupType}

import java.util

object DescriptorMixIns {


  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[AggregationDescriptor[_, _]], name = "AggregationDescriptor"),
    new JsonSubTypes.Type(value = classOf[ConsumerDescriptor[_]], name = "ConsumerDescriptor"),
    new JsonSubTypes.Type(value = classOf[FlatMapDescriptor[_, _]], name = "FlatMapDescriptor"),
    new JsonSubTypes.Type(value = classOf[MapPartitionsDescriptor[_, _]], name = "MapPartitionsDescriptor"),
    new JsonSubTypes.Type(value = classOf[PredicateDescriptor[_]], name = "PredicateDescriptor"),
    new JsonSubTypes.Type(value = classOf[ReduceDescriptor[_]], name = "ReduceDescriptor"),
    new JsonSubTypes.Type(value = classOf[TransformationDescriptor[_, _]], name = "TransformationDescriptor"),
  ))
  abstract class FunctionDescriptorMixIn {
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class PredicateDescriptorMixIn[Input] {
    @JsonCreator
    def this(@JsonProperty("javaImplementation") javaImplementation: SerializablePredicate[Input],
             @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
             @JsonProperty("selectivity") selectivity: ProbabilisticDoubleInterval,
             @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ProjectionDescriptor[_, _]], name = "ProjectionDescriptor"),
  ))
  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class TransformationDescriptorMixIn[Input, Output] {
    @JsonCreator def this(@JsonProperty("javaImplementation") javaImplementation: FunctionDescriptor.SerializableFunction[Input, Output],
                          @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
                          @JsonProperty("outputType") outputType: BasicDataUnitType[Output],
                          @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class ProjectionDescriptorMixIn[Input, Output] {
    @JsonCreator def this(@JsonProperty("javaImplementation") javaImplementation: FunctionDescriptor.SerializableFunction[Input, Output],
                          @JsonProperty("fieldNames") fieldNames: util.List[String],
                          @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
                          @JsonProperty("outputType") outputType: BasicDataUnitType[Output]) = {
      this()
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class ReduceDescriptorMixIn[Type] {
    @JsonCreator
    def this(@JsonProperty("javaImplementation") javaImplementation: FunctionDescriptor.SerializableBinaryOperator[Type],
             @JsonProperty("inputType") inputType: DataUnitGroupType[Type],
             @JsonProperty("outputType") outputType: BasicDataUnitType[Type],
             @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class FlatMapDescriptorMixIn[Input, Output] {
    @JsonCreator
    def this(@JsonProperty("javaImplementation") javaImplementation: FunctionDescriptor.SerializableFunction[Input, Iterable[Output]],
             @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
             @JsonProperty("outputType") outputType: BasicDataUnitType[Output],
             @JsonProperty("selectivity") selectivity: ProbabilisticDoubleInterval,
             @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class MapPartitionsDescriptorMixIn[Input, Output] {
    @JsonCreator
    def this(@JsonProperty("javaImplementation") javaImplementation: FunctionDescriptor.SerializableFunction[Iterable[Input], Iterable[Output]],
             @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
             @JsonProperty("outputType") outputType: BasicDataUnitType[Output],
             @JsonProperty("selectivity") selectivity: ProbabilisticDoubleInterval,
             @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

}