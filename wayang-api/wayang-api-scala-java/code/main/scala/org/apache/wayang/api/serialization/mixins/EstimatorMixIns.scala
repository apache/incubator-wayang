package org.apache.wayang.api.serialization.mixins

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonSubTypes, JsonTypeInfo, JsonTypeName}
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.core.function.FunctionDescriptor
import org.apache.wayang.core.function.FunctionDescriptor.SerializableToDoubleBiFunction
import org.apache.wayang.core.optimizer.cardinality.{AggregatingCardinalityEstimator, CardinalityEstimate, DefaultCardinalityEstimator, FallbackCardinalityEstimator, FixedSizeCardinalityEstimator, SwitchForwardCardinalityEstimator}
import org.apache.wayang.core.optimizer.costs.{ConstantLoadProfileEstimator, DefaultLoadEstimator, IntervalLoadEstimator, LoadEstimator, NestableLoadProfileEstimator}

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

}