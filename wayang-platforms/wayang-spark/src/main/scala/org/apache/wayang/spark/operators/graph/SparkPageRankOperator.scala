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

package org.apache.wayang.spark.operators.graph

import java.lang.{Long => JavaLong}
import java.util
import java.util.Collections

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.PageRank
import org.apache.wayang.basic.data.{Tuple2 => T2}
import org.apache.wayang.basic.operators.PageRankOperator
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators
import org.apache.wayang.core.optimizer.{OptimizationContext, ProbabilisticDoubleInterval}
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode
import org.apache.wayang.core.platform.{ChannelDescriptor, ChannelInstance}
import org.apache.wayang.spark.channels.RddChannel
import org.apache.wayang.spark.execution.SparkExecutor
import org.apache.wayang.spark.operators.SparkExecutionOperator

/**
  * GraphX-based implementation of the [[PageRankOperator]].
  */
class SparkPageRankOperator(_numIterations: Int,
                            _dampingFactor: Float,
                            _graphDensity: ProbabilisticDoubleInterval)
  extends PageRankOperator(_numIterations, _dampingFactor, _graphDensity) with SparkExecutionOperator {

  def this(that: PageRankOperator) = this(that.getNumIterations, that.getDampingFactor, that.getGraphDensity)

  override def evaluate(inputs: Array[ChannelInstance],
                        outputs: Array[ChannelInstance],
                        sparkExecutor: SparkExecutor,
                        operatorContext: OptimizationContext#OperatorContext) = {
    val input = inputs(0).asInstanceOf[RddChannel#Instance]
    val output = outputs(0).asInstanceOf[RddChannel#Instance]

    val edgeRdd = input.provideRdd[T2[JavaLong, JavaLong]]().rdd
      .map(edge => (edge.field0.longValue, edge.field1.longValue))
    val graph = Graph.fromEdgeTuples(edgeRdd, null)
    val prGraph = PageRank.run(graph, this.numIterations, 1d - this.dampingFactor)
    val resultRdd = prGraph.vertices
      .map { case (vertexId, pageRank) => new T2(vertexId, pageRank.toFloat) }
      .toJavaRDD

    output.accept(resultRdd, sparkExecutor)

    val mainExecutionLineageNode = new ExecutionLineageNode(operatorContext)
    mainExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
      "wayang.spark.pagerank.load.main", sparkExecutor.getConfiguration
    ))
    mainExecutionLineageNode.addPredecessor(input.getLineage)

    val outputExecutionLineageNode = new ExecutionLineageNode(operatorContext)
    outputExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
      "wayang.spark.pagerank.load.output", sparkExecutor.getConfiguration
    ))
    output.getLineage.addPredecessor(outputExecutionLineageNode)

    mainExecutionLineageNode.collectAndMark()
  }

  override def getLoadProfileEstimatorConfigurationKeys: java.util.Collection[String] =
    java.util.Arrays.asList("wayang.spark.pagerank.load.main", "wayang.spark.pagerank.load.output")

  override def getSupportedInputChannels(index: Int): util.List[ChannelDescriptor] = {
    assert(index == 0)
    Collections.singletonList(RddChannel.CACHED_DESCRIPTOR)
  }

  override def getSupportedOutputChannels(index: Int): util.List[ChannelDescriptor] = {
    assert(index == 0)
    Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR)
  }

  override def containsAction(): Boolean = true
}
