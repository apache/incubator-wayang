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
import org.apache.wayang.basic.operators.{CartesianOperator, CoGroupOperator, CollectionSource, CountOperator, DistinctOperator, DoWhileOperator, FilterOperator, FlatMapOperator, GlobalMaterializedGroupOperator, GlobalReduceOperator, GroupByOperator, IntersectOperator, JoinOperator, LocalCallbackSink, MapOperator, MapPartitionsOperator, MaterializedGroupByOperator, ReduceByOperator, ReduceOperator, RepeatOperator, SampleOperator, SortOperator, TextFileSource, UnionAllOperator, ZipWithIdOperator}
import org.apache.wayang.core.plan.wayangplan.{ActualOperator, BinaryToUnaryOperator, CompositeOperator, ElementaryOperator, ExecutionOperator, LoopHeadOperator, OperatorBase, Subplan, UnarySink, UnarySource, UnaryToUnaryOperator}

object OperatorMixIns {

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[OperatorBase], name = "OperatorBase"),
    new JsonSubTypes.Type(value = classOf[ActualOperator], name = "ActualOperator"),
    new JsonSubTypes.Type(value = classOf[CompositeOperator], name = "CompositeOperator"),
    new JsonSubTypes.Type(value = classOf[LoopHeadOperator], name = "LoopHeadOperator"),
  ))
  abstract class OperatorMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ElementaryOperator], name = "ElementaryOperator"),
    new JsonSubTypes.Type(value = classOf[Subplan], name = "Subplan"),
  ))
  abstract class ActualOperatorMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[UnarySource[_]], name = "UnarySource"),
    new JsonSubTypes.Type(value = classOf[UnarySink[_]], name = "UnarySink"),
    new JsonSubTypes.Type(value = classOf[UnaryToUnaryOperator[_, _]], name = "UnaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[BinaryToUnaryOperator[_, _, _]], name = "BinaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[DoWhileOperator[_, _]], name = "DoWhileOperator"),
    new JsonSubTypes.Type(value = classOf[RepeatOperator[_]], name = "RepeatOperator"),
  ))
  abstract class OperatorBaseMixIn {
    @JsonIgnore
    def getOriginal(): ExecutionOperator

    @JsonIgnore
    private var original: ExecutionOperator = _
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[UnarySource[_]], name = "UnarySource"),
    new JsonSubTypes.Type(value = classOf[UnarySink[_]], name = "UnarySink"),
    new JsonSubTypes.Type(value = classOf[UnaryToUnaryOperator[_, _]], name = "UnaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[BinaryToUnaryOperator[_, _, _]], name = "BinaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[DoWhileOperator[_, _]], name = "DoWhileOperator"),
    new JsonSubTypes.Type(value = classOf[RepeatOperator[_]], name = "RepeatOperator"),
  ))
  abstract class ElementaryOperatorMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[TextFileSource], name = "TextFileSource"),
    new JsonSubTypes.Type(value = classOf[CollectionSource[_]], name = "CollectionSource"),
  ))
  abstract class UnarySourceMixIn[T] {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[LocalCallbackSink[_]], name = "LocalCallbackSink"),
  ))
  abstract class UnarySinkMixIn[T] {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[MapOperator[_, _]], name = "MapOperator"),
    new JsonSubTypes.Type(value = classOf[MapPartitionsOperator[_, _]], name = "MapPartitionsOperator"),
    new JsonSubTypes.Type(value = classOf[FilterOperator[_]], name = "FilterOperator"),
    new JsonSubTypes.Type(value = classOf[FlatMapOperator[_, _]], name = "FlatMapOperator"),
    new JsonSubTypes.Type(value = classOf[SampleOperator[_]], name = "SampleOperator"),
    new JsonSubTypes.Type(value = classOf[ReduceByOperator[_, _]], name = "ReduceByOperator"),
    new JsonSubTypes.Type(value = classOf[MaterializedGroupByOperator[_, _]], name = "MaterializedGroupByOperator"),
    new JsonSubTypes.Type(value = classOf[GlobalReduceOperator[_]], name = "GlobalReduceOperator"),
    new JsonSubTypes.Type(value = classOf[GlobalMaterializedGroupOperator[_]], name = "GlobalMaterializedGroupOperator"),
    new JsonSubTypes.Type(value = classOf[GroupByOperator[_, _]], name = "GroupByOperator"),
    new JsonSubTypes.Type(value = classOf[ReduceOperator[_]], name = "ReduceOperator"),
    new JsonSubTypes.Type(value = classOf[SortOperator[_, _]], name = "SortOperator"),
    new JsonSubTypes.Type(value = classOf[ZipWithIdOperator[_]], name = "ZipWithIdOperator"),
    new JsonSubTypes.Type(value = classOf[DistinctOperator[_]], name = "DistinctOperator"),
    new JsonSubTypes.Type(value = classOf[CountOperator[_]], name = "CountOperator"),
  ))
  abstract class UnaryToUnaryOperatorMixIn[InputType, OutputType] {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[CartesianOperator[_, _]], name = "CartesianOperator"),
    new JsonSubTypes.Type(value = classOf[UnionAllOperator[_]], name = "UnionAllOperator"),
    new JsonSubTypes.Type(value = classOf[IntersectOperator[_]], name = "IntersectOperator"),
    new JsonSubTypes.Type(value = classOf[JoinOperator[_, _, _]], name = "JoinOperator"),
    new JsonSubTypes.Type(value = classOf[CoGroupOperator[_, _, _]], name = "CoGroupOperator"),
  ))
  abstract class BinaryToUnaryOperatorMixIn[InputType0, InputType1, OutputType] {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[DoWhileOperator[_, _]], name = "DoWhileOperator"),
    new JsonSubTypes.Type(value = classOf[RepeatOperator[_]], name = "RepeatOperator"),
  ))
  abstract class LoopHeadOperatorMixIn {
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class OperatorPatternMixin {
  }

}