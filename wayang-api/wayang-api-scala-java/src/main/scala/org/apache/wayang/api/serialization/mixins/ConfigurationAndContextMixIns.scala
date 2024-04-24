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

import com.fasterxml.jackson.annotation.{JsonIdentityInfo, JsonIgnore, JsonSubTypes, JsonTypeInfo, ObjectIdGenerators}
import org.apache.logging.log4j.Logger
import org.apache.wayang.api.{MultiContext, DataQuanta, PlanBuilder}
import org.apache.wayang.core.api.configuration.{CollectionProvider, ExplicitCollectionProvider, KeyValueProvider, MapBasedKeyValueProvider, ValueProvider}
import org.apache.wayang.core.function.FunctionDescriptor
import org.apache.wayang.core.mapping.Mapping
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.optimizer.channels.ChannelConversion
import org.apache.wayang.core.optimizer.costs.{LoadProfileEstimator, LoadProfileToTimeConverter, TimeToCostConverter}
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerationPruningStrategy
import org.apache.wayang.core.plan.wayangplan.{ExecutionOperator, OutputSlot}
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.profiling.{CardinalityRepository, InstrumentationStrategy}

import java.util.function.ToDoubleFunction

object ConfigurationAndContextMixIns {


  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[MultiContext], name = "MultiContext"),
  ))
  abstract class WayangContextMixIn {
    @JsonIgnore
    private var logger: Logger = _

    // TODO: Is this okay?
    @JsonIgnore
    private var cardinalityRepository: CardinalityRepository = _
  }




  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ConfigurationMixIn {
    @JsonIgnore
    private var cardinalityEstimatorProvider: KeyValueProvider[OutputSlot[_], CardinalityEstimator] = _

    @JsonIgnore
    private var udfSelectivityProvider: KeyValueProvider[FunctionDescriptor, ProbabilisticDoubleInterval] = _

    @JsonIgnore
    private var operatorLoadProfileEstimatorProvider: KeyValueProvider[ExecutionOperator, LoadProfileEstimator] = _

    @JsonIgnore
    private var functionLoadProfileEstimatorProvider: KeyValueProvider[FunctionDescriptor, LoadProfileEstimator] = _

    @JsonIgnore
    private var loadProfileEstimatorCache: MapBasedKeyValueProvider[String, LoadProfileEstimator] = _

    @JsonIgnore
    private var loadProfileToTimeConverterProvider: KeyValueProvider[Platform, LoadProfileToTimeConverter] = _

    @JsonIgnore
    private var timeToCostConverterProvider: KeyValueProvider[Platform, TimeToCostConverter] = _

    @JsonIgnore
    private var costSquasherProvider: ValueProvider[ToDoubleFunction[ProbabilisticDoubleInterval]] = _

    @JsonIgnore
    private var platformStartUpTimeProvider: KeyValueProvider[Platform, Long] = _

    @JsonIgnore
    private var platformProvider: ExplicitCollectionProvider[Platform] = _

    @JsonIgnore
    private var mappingProvider: ExplicitCollectionProvider[Mapping] = _

    @JsonIgnore
    private var channelConversionProvider: ExplicitCollectionProvider[ChannelConversion] = _

    @JsonIgnore
    private var pruningStrategyClassProvider: CollectionProvider[Class[PlanEnumerationPruningStrategy]] = _

    @JsonIgnore
    private var instrumentationStrategyProvider: ValueProvider[InstrumentationStrategy] = _
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[MultiContext.TextFileSink], name = "MultiContextTextFileSink"),
    new JsonSubTypes.Type(value = classOf[MultiContext.ObjectFileSink], name = "MultiContextObjectFileSink"
    ))
  )
  abstract class MultiContextUnarySinkMixIn {
  }

  abstract class MultiContextPlanBuilderMixIn {
    @JsonIgnore
    private var multiContextMap: Map[Long, MultiContext] = _

    @JsonIgnore
    private var dataQuantaMap: Map[Long, DataQuanta[_]] = _

    @JsonIgnore
    private var planBuilderMap: Map[Long, PlanBuilder] = _
  }

}