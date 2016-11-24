package org.qcri.rheem.spark.operators.graph

import java.lang.{Long => JavaLong}
import java.util
import java.util.Collections

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.PageRank
import org.qcri.rheem.basic.data.{Tuple2 => T2}
import org.qcri.rheem.basic.operators.PageRankOperator
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators
import org.qcri.rheem.core.optimizer.{OptimizationContext, ProbabilisticDoubleInterval}
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode
import org.qcri.rheem.core.platform.{ChannelDescriptor, ChannelInstance}
import org.qcri.rheem.spark.channels.RddChannel
import org.qcri.rheem.spark.execution.SparkExecutor
import org.qcri.rheem.spark.operators.SparkExecutionOperator

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
      "rheem.spark.pagerank.load.main", sparkExecutor.getConfiguration
    ))
    mainExecutionLineageNode.addPredecessor(input.getLineage)

    val outputExecutionLineageNode = new ExecutionLineageNode(operatorContext)
    outputExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
      "rheem.spark.pagerank.load.output", sparkExecutor.getConfiguration
    ))
    output.getLineage.addPredecessor(outputExecutionLineageNode)

    mainExecutionLineageNode.collectAndMark()
  }

  override def getLoadProfileEstimatorConfigurationKeys: java.util.Collection[String] =
    java.util.Arrays.asList("rheem.spark.pagerank.load.main", "rheem.spark.pagerank.load.output")

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
