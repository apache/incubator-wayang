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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIdentityInfo, JsonIgnore, JsonProperty, JsonSetter, JsonSubTypes, JsonTypeInfo, ObjectIdGenerators}
import org.apache.logging.log4j.Logger
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.core.api.configuration.{ExplicitCollectionProvider, FunctionalCollectionProvider, FunctionalKeyValueProvider, KeyValueProvider, MapBasedKeyValueProvider}

import java.util.function.BiFunction

object ProviderMixIns {

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class FunctionalKeyValueProviderMixIn[Key, Value] {
    @JsonCreator
    def this(@JsonProperty("parent") parent: KeyValueProvider[Key, Value],
             @JsonProperty("configuration") configuration: Configuration,
             @JsonProperty("providerFunction") providerFunction: BiFunction[Key, KeyValueProvider[Key, Value], Value]) = {
      this()
    }
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class MapBasedKeyValueProviderMixIn[Key, Value] {
    @JsonSetter("storedValues")
    private def setStoredValues(storedValues: Map[Key, Value]): Unit = {}

    @JsonCreator
    def this(@JsonProperty("parent") parent: KeyValueProvider[Key, Value],
             @JsonProperty("configuration") configuration: Configuration,
             @JsonProperty("isCaching") isCaching: Boolean) = {
      this()
    }
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ConstantValueProviderMixIn {
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ExplicitCollectionProviderMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[FunctionalKeyValueProvider[_, _]], name = "FunctionalKeyValueProvider"),
    new JsonSubTypes.Type(value = classOf[MapBasedKeyValueProvider[_, _]], name = "MapBasedKeyValueProvider"
    ))
  )
  abstract class KeyValueProviderMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ExplicitCollectionProvider[_]], name = "ExplicitCollectionProvider"),
    new JsonSubTypes.Type(value = classOf[FunctionalCollectionProvider[_]], name = "FunctionalCollectionProvider"
    ))
  )
  abstract class CollectionProviderMixIn {
  }

}