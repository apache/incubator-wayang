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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonSubTypes, JsonTypeInfo, JsonTypeName}
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.core.function.FunctionDescriptor
import org.apache.wayang.core.function.FunctionDescriptor.SerializableToDoubleBiFunction
import org.apache.wayang.core.optimizer.cardinality.{AggregatingCardinalityEstimator, CardinalityEstimate, DefaultCardinalityEstimator, FallbackCardinalityEstimator, FixedSizeCardinalityEstimator, SwitchForwardCardinalityEstimator}
import org.apache.wayang.core.optimizer.costs.{ConstantLoadProfileEstimator, DefaultEstimatableCost, DefaultLoadEstimator, IntervalLoadEstimator, LoadEstimator, NestableLoadProfileEstimator}

object EstimatorMixIns {


  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[DefaultLoadEstimator], name = "DefaultLoadEstimator"),
    new JsonSubTypes.Type(value = classOf[IntervalLoadEstimator], name = "IntervalLoadEstimator"),
  ))
  abstract class LoadEstimatorMixIn {
  }

  abstract class DefaultLoadEstimatorMixIn {
    @JsonCreator
    def this(@JsonProperty("numInputs") numInputs: Int,
             @JsonProperty("numOutputs") numOutputs: Int,
             @JsonProperty("correctnessProbability") correctnessProbability: Double,
             @JsonProperty("nullCardinalityReplacement") nullCardinalityReplacement: CardinalityEstimate,
             @JsonProperty("singlePointFunction") singlePointFunction: LoadEstimator.SinglePointEstimationFunction) = {
      this()
    }
  }

  abstract class CardinalityEstimateMixIn {
    @JsonCreator
    def this(@JsonProperty("lowerEstimate") lowerEstimate: Long,
             @JsonProperty("upperEstimate") upperEstimate: Long,
             @JsonProperty("correctnessProb") correctnessProb: Double,
             @JsonProperty("isOverride") isOverride: Boolean) = {
      this()
    }
  }


  abstract class ProbabilisticDoubleIntervalMixIn {
    @JsonCreator
    def this(@JsonProperty("lowerEstimate") lowerEstimate: Double,
             @JsonProperty("upperEstimate") upperEstimate: Double,
             @JsonProperty("correctnessProb") correctnessProb: Double,
             @JsonProperty("isOverride") isOverride: Boolean) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ConstantLoadProfileEstimator], name = "ConstantLoadProfileEstimator"),
    new JsonSubTypes.Type(value = classOf[NestableLoadProfileEstimator], name = "NestableLoadProfileEstimator"),
  ))
  abstract class LoadProfileEstimatorMixIn {
  }

  @JsonTypeName("nestableLoadProfileEstimator")
  abstract class NestableLoadProfileEstimatorMixIn {
    @JsonCreator
    def this (@JsonProperty("cpuLoadEstimator") cpuLoadEstimator : LoadEstimator,
              @JsonProperty("ramLoadEstimator") ramLoadEstimator: LoadEstimator,
              @JsonProperty("diskLoadEstimator") diskLoadEstimator: LoadEstimator,
              @JsonProperty("networkLoadEstimator") networkLoadEstimator: LoadEstimator,
              @JsonProperty("resourceUtilizationEstimator") resourceUtilizationEstimator: SerializableToDoubleBiFunction[Array[Long], Array[Long]],
              @JsonProperty("overheadMillis") overheadMillis: Long,
              @JsonProperty("configurationKey") configurationKey: String
             ) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[AggregatingCardinalityEstimator], name = "AggregatingCardinalityEstimator"),
    new JsonSubTypes.Type(value = classOf[DefaultCardinalityEstimator], name = "DefaultCardinalityEstimator"),
    new JsonSubTypes.Type(value = classOf[FallbackCardinalityEstimator], name = "FallbackCardinalityEstimator"),
    new JsonSubTypes.Type(value = classOf[FixedSizeCardinalityEstimator], name = "FixedSizeCardinalityEstimator"),
    new JsonSubTypes.Type(value = classOf[SwitchForwardCardinalityEstimator], name = "SwitchForwardCardinalityEstimator"),
  ))
  abstract class CardinalityEstimatorMixIn {
  }

  abstract class DefaultCardinalityEstimatorMixIn {
    @JsonCreator
    def this(@JsonProperty("certaintyProb") certaintyProb: Double,
             @JsonProperty("numInputs") numInputs: Int,
             @JsonProperty("isAllowMoreInputs") isAllowMoreInputs: Boolean,
             @JsonProperty("singlePointEstimator") singlePointEstimator: FunctionDescriptor.SerializableToLongBiFunction[Array[Long], Configuration]) = {
      this()
    }
  }

  // TODO: Add more estimator mixins

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[DefaultEstimatableCost], name = "DefaultEstimatableCost"),
  ))
  abstract class EstimatableCostMixIn {
  }

}