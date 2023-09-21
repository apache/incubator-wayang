# Detailed Wayang Platform Documentation

## Introduction

This document provides a detailed overview of the Wayang platform's modules, focusing on the Java classes within each module.

## wayang-api

### Java Classes:

- **PythonAPI**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/PythonAPI.java`.

- **ProcessFeeder**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/executor/ProcessFeeder.java`.

- **ProcessReceiver**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/executor/ProcessReceiver.java`.

- **PythonProcessCaller**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/executor/PythonProcessCaller.java`.

- **PythonWorkerManager**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/executor/PythonWorkerManager.java`.

- **ReaderIterator**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/executor/ReaderIterator.java`.

- **PythonCode**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/function/PythonCode.java`.

- **PythonFunctionWrapper**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/function/PythonFunctionWrapper.java`.

- **PythonUDF**: Located at `wayang-api/wayang-api-python/src/main/java/org/apache/wayang/api/python/function/PythonUDF.java`.

- **PythonAPITest**: Located at `wayang-api/wayang-api-python/src/test/java/org/apache/wayang/api/python/PythonAPITest.java`.

- **WayangPlanBuilder**: Located at `wayang-api/wayang-api-rest/src/main/java/org/apache/wayang/api/rest/server/spring/decoder/WayangPlanBuilder.java`.

- **WayangController**: Located at `wayang-api/wayang-api-rest/src/main/java/org/apache/wayang/api/rest/server/spring/general/WayangController.java`.

- **JavaApiTest**: Located at `wayang-api/wayang-api-scala-java/code/test/java/org/apache/wayang/api/JavaApiTest.java`.

- **WayangConvention**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/convention/WayangConvention.java`.

- **WayangFilterVisitor**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/converter/WayangFilterVisitor.java`.

- **WayangJoinVisitor**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/converter/WayangJoinVisitor.java`.

- **WayangProjectVisitor**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/converter/WayangProjectVisitor.java`.

- **WayangRelConverter**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/converter/WayangRelConverter.java`.

- **WayangRelNodeVisitor**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/converter/WayangRelNodeVisitor.java`.

- **WayangTableScanVisitor**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/converter/WayangTableScanVisitor.java`.

- **JdbcSchema**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/jdbc/JdbcSchema.java`.

- **JdbcTable**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/jdbc/JdbcTable.java`.

- **JdbcUtils**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/jdbc/JdbcUtils.java`.

- **Optimizer**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/optimizer/Optimizer.java`.

- **WayangProgram**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/optimizer/WayangProgram.java`.

- **WayangFilter**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/rel/WayangFilter.java`.

- **WayangJoin**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/rel/WayangJoin.java`.

- **WayangProject**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/rel/WayangProject.java`.

- **WayangRel**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/rel/WayangRel.java`.

- **WayangTableScan**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/rel/WayangTableScan.java`.

- **WayangRules**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/rules/WayangRules.java`.

- **SchemaUtils**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/schema/SchemaUtils.java`.

- **WayangSchema**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/schema/WayangSchema.java`.

- **WayangSchemaBuilder**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/schema/WayangSchemaBuilder.java`.

- **WayangTable**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/schema/WayangTable.java`.

- **WayangTableBuilder**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/schema/WayangTableBuilder.java`.

- **WayangTableStatistic**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/schema/WayangTableStatistic.java`.

- **ModelParser**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/utils/ModelParser.java`.

- **PrintUtils**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/calcite/utils/PrintUtils.java`.

- **SqlContext**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/context/SqlContext.java`.

- **CsvRowConverter**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/sources/fs/CsvRowConverter.java`.

- **JavaCSVTableSource**: Located at `wayang-api/wayang-api-sql/src/main/java/org/apache/wayang/api/sql/sources/fs/JavaCSVTableSource.java`.

- **SqlAPI**: Located at `wayang-api/wayang-api-sql/src/test/java/org/apache/wayang/api/sql/SqlAPI.java`.

- **SqlTest**: Located at `wayang-api/wayang-api-sql/src/test/java/org/apache/wayang/api/sql/SqlTest.java`.

- **SqlToWayangRelTest**: Located at `wayang-api/wayang-api-sql/src/test/java/org/apache/wayang/api/sql/SqlToWayangRelTest.java`.



## wayang-assembly

No main Java classes identified.



## wayang-benchmark

### Java Classes:

- **Grep**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/grep/Grep.java`.

- **SGDImpl**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/sgd/SGDImpl.java`.

- **SGDImprovedImpl**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/sgd/SGDImprovedImpl.java`.

- **Random16**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/terasort/Random16.java`.

- **Unsigned16**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/terasort/Unsigned16.java`.

- **Main**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/wordcount/Main.java`.

- **LineItemTuple**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/tpch/data/LineItemTuple.java`.

- **GroupKey**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/tpch/data/q1/GroupKey.java`.

- **ReturnTuple**: Located at `wayang-benchmark/code/main/java/org/apache/wayang/apps/tpch/data/q1/ReturnTuple.java`.

- **LineItemTupleTest**: Located at `wayang-benchmark/code/test/java/org/apache/wayang/apps/tpch/data/LineItemTupleTest.java`.



## wayang-commons

### Java Classes:

- **WayangBasics**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/WayangBasics.java`.

- **FileChannel**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/channels/FileChannel.java`.

- **Record**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/data/Record.java`.

- **Tuple2**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/data/Tuple2.java`.

- **Tuple5**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/data/Tuple5.java`.

- **ProjectionDescriptor**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/function/ProjectionDescriptor.java`.

- **GlobalReduceMapping**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/mapping/GlobalReduceMapping.java`.

- **Mappings**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/mapping/Mappings.java`.

- **MaterializedGroupByMapping**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/mapping/MaterializedGroupByMapping.java`.

- **PageRankMapping**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/mapping/PageRankMapping.java`.

- **ReduceByMapping**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/mapping/ReduceByMapping.java`.

- **RepeatMapping**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/mapping/RepeatMapping.java`.

- **CartesianOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/CartesianOperator.java`.

- **CoGroupOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/CoGroupOperator.java`.

- **CollectionSource**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/CollectionSource.java`.

- **CountOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/CountOperator.java`.

- **DistinctOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/DistinctOperator.java`.

- **DoWhileOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/DoWhileOperator.java`.

- **FilterOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/FilterOperator.java`.

- **FlatMapOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/FlatMapOperator.java`.

- **GlobalMaterializedGroupOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/GlobalMaterializedGroupOperator.java`.

- **GlobalReduceOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/GlobalReduceOperator.java`.

- **GroupByOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/GroupByOperator.java`.

- **IntersectOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/IntersectOperator.java`.

- **JoinOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/JoinOperator.java`.

- **LocalCallbackSink**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/LocalCallbackSink.java`.

- **LoopOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/LoopOperator.java`.

- **MapOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/MapOperator.java`.

- **MapPartitionsOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/MapPartitionsOperator.java`.

- **MaterializedGroupByOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/MaterializedGroupByOperator.java`.

- **ObjectFileSink**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/ObjectFileSink.java`.

- **ObjectFileSource**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/ObjectFileSource.java`.

- **PageRankOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/PageRankOperator.java`.

- **ReduceByOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/ReduceByOperator.java`.

- **ReduceOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/ReduceOperator.java`.

- **RepeatOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/RepeatOperator.java`.

- **SampleOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/SampleOperator.java`.

- **SortOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/SortOperator.java`.

- **TableSource**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/TableSource.java`.

- **TextFileSink**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/TextFileSink.java`.

- **TextFileSource**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/TextFileSource.java`.

- **UnionAllOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/UnionAllOperator.java`.

- **ZipWithIdOperator**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/ZipWithIdOperator.java`.

- **WayangBasic**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/plugin/WayangBasic.java`.

- **WayangBasicGraph**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/plugin/WayangBasicGraph.java`.

- **RecordType**: Located at `wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/types/RecordType.java`.

- **ProjectionDescriptorTest**: Located at `wayang-commons/wayang-basic/src/test/java/org/apache/wayang/basic/function/ProjectionDescriptorTest.java`.

- **ReduceByMappingTest**: Located at `wayang-commons/wayang-basic/src/test/java/org/apache/wayang/basic/mapping/ReduceByMappingTest.java`.

- **MaterializedGroupByOperatorTest**: Located at `wayang-commons/wayang-basic/src/test/java/org/apache/wayang/basic/operators/MaterializedGroupByOperatorTest.java`.

- **TextFileSourceTest**: Located at `wayang-commons/wayang-basic/src/test/java/org/apache/wayang/basic/operators/TextFileSourceTest.java`.

- **TestSink**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestSink.java`.

- **TestSource**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestSource.java`.

- **RecordTypeTest**: Located at `wayang-commons/wayang-basic/src/test/java/org/apache/wayang/basic/types/RecordTypeTest.java`.

- **Configuration**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/Configuration.java`.

- **Job**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/Job.java`.

- **WayangContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/WayangContext.java`.

- **CollectionProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/CollectionProvider.java`.

- **ConstantValueProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/ConstantValueProvider.java`.

- **ExplicitCollectionProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/ExplicitCollectionProvider.java`.

- **FunctionalCollectionProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/FunctionalCollectionProvider.java`.

- **FunctionalKeyValueProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/FunctionalKeyValueProvider.java`.

- **FunctionalValueProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/FunctionalValueProvider.java`.

- **KeyValueProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/KeyValueProvider.java`.

- **MapBasedKeyValueProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/MapBasedKeyValueProvider.java`.

- **ValueProvider**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/configuration/ValueProvider.java`.

- **WayangException**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/exception/WayangException.java`.

- **AggregationDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/AggregationDescriptor.java`.

- **ConsumerDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/ConsumerDescriptor.java`.

- **ExecutionContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/ExecutionContext.java`.

- **ExtendedFunction**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/ExtendedFunction.java`.

- **FlatMapDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/FlatMapDescriptor.java`.

- **FunctionDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/FunctionDescriptor.java`.

- **MapPartitionsDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/MapPartitionsDescriptor.java`.

- **PredicateDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/PredicateDescriptor.java`.

- **ReduceDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/ReduceDescriptor.java`.

- **TransformationDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/TransformationDescriptor.java`.

- **Mapping**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/Mapping.java`.

- **OperatorMatch**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/OperatorMatch.java`.

- **OperatorPattern**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/OperatorPattern.java`.

- **PlanTransformation**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/PlanTransformation.java`.

- **ReplacementSubplanFactory**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/ReplacementSubplanFactory.java`.

- **SubplanMatch**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/SubplanMatch.java`.

- **SubplanPattern**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/mapping/SubplanPattern.java`.

- **DisabledMonitor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/monitor/DisabledMonitor.java`.

- **FileMonitor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/monitor/FileMonitor.java`.

- **HttpMonitor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/monitor/HttpMonitor.java`.

- **Monitor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/monitor/Monitor.java`.

- **ZeroMQMonitor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/monitor/ZeroMQMonitor.java`.

- **AggregateOptimizationContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/AggregateOptimizationContext.java`.

- **DefaultOptimizationContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/DefaultOptimizationContext.java`.

- **OptimizationContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/OptimizationContext.java`.

- **OptimizationUtils**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/OptimizationUtils.java`.

- **ProbabilisticDoubleInterval**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/ProbabilisticDoubleInterval.java`.

- **ProbabilisticIntervalEstimate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/ProbabilisticIntervalEstimate.java`.

- **SanityChecker**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/SanityChecker.java`.

- **AbstractAlternativeCardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/AbstractAlternativeCardinalityPusher.java`.

- **AggregatingCardinalityEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/AggregatingCardinalityEstimator.java`.

- **CardinalityEstimate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityEstimate.java`.

- **CardinalityEstimationTraversal**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityEstimationTraversal.java`.

- **CardinalityEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityEstimator.java`.

- **CardinalityEstimatorManager**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityEstimatorManager.java`.

- **CardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityPusher.java`.

- **DefaultCardinalityEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/DefaultCardinalityEstimator.java`.

- **DefaultCardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/DefaultCardinalityPusher.java`.

- **FallbackCardinalityEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/FallbackCardinalityEstimator.java`.

- **FixedSizeCardinalityEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/FixedSizeCardinalityEstimator.java`.

- **LoopHeadAlternativeCardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/LoopHeadAlternativeCardinalityPusher.java`.

- **LoopSubplanCardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/LoopSubplanCardinalityPusher.java`.

- **OperatorAlternativeCardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/OperatorAlternativeCardinalityPusher.java`.

- **SubplanCardinalityPusher**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/SubplanCardinalityPusher.java`.

- **SwitchForwardCardinalityEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/SwitchForwardCardinalityEstimator.java`.

- **ChannelConversion**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/channels/ChannelConversion.java`.

- **ChannelConversionGraph**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/channels/ChannelConversionGraph.java`.

- **DefaultChannelConversion**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/channels/DefaultChannelConversion.java`.

- **ConstantLoadProfileEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/ConstantLoadProfileEstimator.java`.

- **DefaultLoadEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/DefaultLoadEstimator.java`.

- **EstimationContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/EstimationContext.java`.

- **IntervalLoadEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/IntervalLoadEstimator.java`.

- **LoadEstimate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadEstimate.java`.

- **LoadEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadEstimator.java`.

- **LoadProfile**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadProfile.java`.

- **LoadProfileEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadProfileEstimator.java`.

- **LoadProfileEstimators**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadProfileEstimators.java`.

- **LoadProfileToTimeConverter**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadProfileToTimeConverter.java`.

- **LoadToTimeConverter**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadToTimeConverter.java`.

- **NestableLoadProfileEstimator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/NestableLoadProfileEstimator.java`.

- **SimpleEstimationContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/SimpleEstimationContext.java`.

- **TimeEstimate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/TimeEstimate.java`.

- **TimeToCostConverter**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/TimeToCostConverter.java`.

- **ExecutionTaskFlow**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/ExecutionTaskFlow.java`.

- **ExecutionTaskFlowCompiler**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/ExecutionTaskFlowCompiler.java`.

- **LatentOperatorPruningStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/LatentOperatorPruningStrategy.java`.

- **LoopEnumerator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/LoopEnumerator.java`.

- **LoopImplementation**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/LoopImplementation.java`.

- **PlanEnumeration**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/PlanEnumeration.java`.

- **PlanEnumerationPruningStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/PlanEnumerationPruningStrategy.java`.

- **PlanEnumerator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/PlanEnumerator.java`.

- **PlanImplementation**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/PlanImplementation.java`.

- **RandomPruningStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/RandomPruningStrategy.java`.

- **SinglePlatformPruningStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/SinglePlatformPruningStrategy.java`.

- **StageAssignmentTraversal**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/StageAssignmentTraversal.java`.

- **TopKPruningStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/enumeration/TopKPruningStrategy.java`.

- **EnumerationAlternative**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/partition/EnumerationAlternative.java`.

- **EnumerationBranch**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/partition/EnumerationBranch.java`.

- **Channel**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/Channel.java`.

- **ChannelInitializer**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/ChannelInitializer.java`.

- **ExecutionPlan**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/ExecutionPlan.java`.

- **ExecutionStage**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/ExecutionStage.java`.

- **ExecutionStageLoop**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/ExecutionStageLoop.java`.

- **ExecutionTask**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/ExecutionTask.java`.

- **PlatformExecution**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/executionplan/PlatformExecution.java`.

- **ActualOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/ActualOperator.java`.

- **BinaryToUnaryOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/BinaryToUnaryOperator.java`.

- **CompositeOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/CompositeOperator.java`.

- **ElementaryOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/ElementaryOperator.java`.

- **EstimationContextProperty**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/EstimationContextProperty.java`.

- **ExecutionOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/ExecutionOperator.java`.

- **InputSlot**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/InputSlot.java`.

- **LoopHeadAlternative**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/LoopHeadAlternative.java`.

- **LoopHeadOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/LoopHeadOperator.java`.

- **LoopIsolator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/LoopIsolator.java`.

- **LoopSubplan**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/LoopSubplan.java`.

- **Operator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/Operator.java`.

- **OperatorAlternative**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorAlternative.java`.

- **OperatorBase**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorBase.java`.

- **OperatorContainer**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorContainer.java`.

- **OperatorContainers**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorContainers.java`.

- **Operators**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/Operators.java`.

- **OutputSlot**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OutputSlot.java`.

- **PlanMetrics**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/PlanMetrics.java`.

- **PlanTraversal**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/PlanTraversal.java`.

- **Slot**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/Slot.java`.

- **SlotMapping**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/SlotMapping.java`.

- **Subplan**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/Subplan.java`.

- **TopDownPlanVisitor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/TopDownPlanVisitor.java`.

- **UnarySink**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/UnarySink.java`.

- **UnarySource**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/UnarySource.java`.

- **UnaryToUnaryOperator**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/UnaryToUnaryOperator.java`.

- **WayangPlan**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/WayangPlan.java`.

- **AbstractTopologicalTraversal**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/traversal/AbstractTopologicalTraversal.java`.

- **AbstractChannelInstance**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/AbstractChannelInstance.java`.

- **AtomicExecution**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/AtomicExecution.java`.

- **AtomicExecutionGroup**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/AtomicExecutionGroup.java`.

- **Breakpoint**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/Breakpoint.java`.

- **CardinalityBreakpoint**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/CardinalityBreakpoint.java`.

- **ChannelDescriptor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ChannelDescriptor.java`.

- **ChannelInstance**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ChannelInstance.java`.

- **CompositeExecutionResource**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/CompositeExecutionResource.java`.

- **ConjunctiveBreakpoint**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ConjunctiveBreakpoint.java`.

- **CrossPlatformExecutor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/CrossPlatformExecutor.java`.

- **ExecutionResource**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ExecutionResource.java`.

- **ExecutionResourceTemplate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ExecutionResourceTemplate.java`.

- **ExecutionState**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ExecutionState.java`.

- **Executor**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/Executor.java`.

- **ExecutorTemplate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/ExecutorTemplate.java`.

- **FixBreakpoint**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/FixBreakpoint.java`.

- **Junction**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/Junction.java`.

- **NoIterationBreakpoint**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/NoIterationBreakpoint.java`.

- **PartialExecution**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/PartialExecution.java`.

- **Platform**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/Platform.java`.

- **PushExecutorTemplate**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/PushExecutorTemplate.java`.

- **ChannelLineageNode**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/lineage/ChannelLineageNode.java`.

- **ExecutionLineageNode**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/lineage/ExecutionLineageNode.java`.

- **LazyExecutionLineageNode**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/platform/lineage/LazyExecutionLineageNode.java`.

- **DynamicPlugin**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plugin/DynamicPlugin.java`.

- **Plugin**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plugin/Plugin.java`.

- **CardinalityRepository**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/CardinalityRepository.java`.

- **CostMeasurement**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/CostMeasurement.java`.

- **ExecutionLog**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/ExecutionLog.java`.

- **ExecutionPlanMeasurement**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/ExecutionPlanMeasurement.java`.

- **FullInstrumentationStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/FullInstrumentationStrategy.java`.

- **InstrumentationStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/InstrumentationStrategy.java`.

- **NoInstrumentationStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/NoInstrumentationStrategy.java`.

- **OutboundInstrumentationStrategy**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/OutboundInstrumentationStrategy.java`.

- **PartialExecutionMeasurement**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/PartialExecutionMeasurement.java`.

- **ProfileDBs**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/profiling/ProfileDBs.java`.

- **BasicDataUnitType**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/BasicDataUnitType.java`.

- **DataSetType**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/DataSetType.java`.

- **DataUnitGroupType**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/DataUnitGroupType.java`.

- **DataUnitType**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/DataUnitType.java`.

- **AbstractReferenceCountable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/AbstractReferenceCountable.java`.

- **Action**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Action.java`.

- **Actions**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Actions.java`.

- **Bitmask**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Bitmask.java`.

- **Canonicalizer**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Canonicalizer.java`.

- **ConsumerIteratorAdapter**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/ConsumerIteratorAdapter.java`.

- **Copyable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Copyable.java`.

- **Counter**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Counter.java`.

- **CrossProductIterable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/CrossProductIterable.java`.

- **Formats**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Formats.java`.

- **Iterators**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Iterators.java`.

- **JsonSerializable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/JsonSerializable.java`.

- **JsonSerializables**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/JsonSerializables.java`.

- **JsonSerializer**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/JsonSerializer.java`.

- **JuelUtils**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/JuelUtils.java`.

- **LimitedInputStream**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/LimitedInputStream.java`.

- **Logging**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Logging.java`.

- **LruCache**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/LruCache.java`.

- **MultiMap**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/MultiMap.java`.

- **OneTimeExecutable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/OneTimeExecutable.java`.

- **Optional**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Optional.java`.

- **ReferenceCountable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/ReferenceCountable.java`.

- **ReflectionUtils**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/ReflectionUtils.java`.

- **Tuple**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/Tuple.java`.

- **WayangArrays**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/WayangArrays.java`.

- **WayangCollections**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/WayangCollections.java`.

- **FileSystem**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/fs/FileSystem.java`.

- **FileSystems**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/fs/FileSystems.java`.

- **FileUtils**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/fs/FileUtils.java`.

- **HadoopFileSystem**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/fs/HadoopFileSystem.java`.

- **LocalFileSystem**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/fs/LocalFileSystem.java`.

- **S3FileSystem**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/fs/S3FileSystem.java`.

- **WayangJsonArray**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/json/WayangJsonArray.java`.

- **WayangJsonObj**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/json/WayangJsonObj.java`.

- **Context**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/Context.java`.

- **DefaultContext**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/DefaultContext.java`.

- **Expression**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/Expression.java`.

- **ExpressionBuilder**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/ExpressionBuilder.java`.

- **EvaluationException**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/exceptions/EvaluationException.java`.

- **MathExException**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/exceptions/MathExException.java`.

- **ParseException**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/exceptions/ParseException.java`.

- **BinaryOperation**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/model/BinaryOperation.java`.

- **CompiledFunction**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/model/CompiledFunction.java`.

- **Constant**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/model/Constant.java`.

- **NamedFunction**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/model/NamedFunction.java`.

- **UnaryOperation**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/model/UnaryOperation.java`.

- **Variable**: Located at `wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/util/mathex/model/Variable.java`.

- **SlotTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/SlotTest.java`.

- **OperatorPatternTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/mapping/OperatorPatternTest.java`.

- **PlanTransformationTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/mapping/PlanTransformationTest.java`.

- **SubplanPatternTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/mapping/SubplanPatternTest.java`.

- **TestSinkMapping**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/mapping/test/TestSinkMapping.java`.

- **TestSinkToTestSink2Factory**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/mapping/test/TestSinkToTestSink2Factory.java`.

- **AggregatingCardinalityEstimatorTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/cardinality/AggregatingCardinalityEstimatorTest.java`.

- **DefaultCardinalityEstimatorTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/cardinality/DefaultCardinalityEstimatorTest.java`.

- **LoopSubplanCardinalityPusherTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/cardinality/LoopSubplanCardinalityPusherTest.java`.

- **SubplanCardinalityPusherTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/cardinality/SubplanCardinalityPusherTest.java`.

- **ChannelConversionGraphTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/channels/ChannelConversionGraphTest.java`.

- **NestableLoadProfileEstimatorTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/costs/NestableLoadProfileEstimatorTest.java`.

- **StageAssignmentTraversalTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/optimizer/enumeration/StageAssignmentTraversalTest.java`.

- **TestChannel**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/executionplan/test/TestChannel.java`.

- **LoopIsolatorTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/LoopIsolatorTest.java`.

- **OperatorTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/OperatorTest.java`.

- **SlotMappingTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/SlotMappingTest.java`.

- **TestCustomMapOperator**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestCustomMapOperator.java`.

- **TestFilterOperator**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestFilterOperator.java`.

- **TestJoin**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestJoin.java`.

- **TestLoopHead**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestLoopHead.java`.

- **TestMapOperator**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestMapOperator.java`.

- **TestSink2**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plan/wayangplan/test/TestSink2.java`.

- **PartialExecutionTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/platform/PartialExecutionTest.java`.

- **DynamicPluginTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/plugin/DynamicPluginTest.java`.

- **DummyExecutionOperator**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/DummyExecutionOperator.java`.

- **DummyExternalReusableChannel**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/DummyExternalReusableChannel.java`.

- **DummyNonReusableChannel**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/DummyNonReusableChannel.java`.

- **DummyPlatform**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/DummyPlatform.java`.

- **DummyReusableChannel**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/DummyReusableChannel.java`.

- **MockFactory**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/MockFactory.java`.

- **SerializableDummyExecutionOperator**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/SerializableDummyExecutionOperator.java`.

- **TestDataUnit**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/TestDataUnit.java`.

- **TestDataUnit2**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/test/TestDataUnit2.java`.

- **BitmaskTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/BitmaskTest.java`.

- **ConsumerIteratorAdapterTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/ConsumerIteratorAdapterTest.java`.

- **CrossProductIterableTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/CrossProductIterableTest.java`.

- **LimitedInputStreamTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/LimitedInputStreamTest.java`.

- **ReflectionUtilsTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/ReflectionUtilsTest.java`.

- **WayangCollectionsTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/WayangCollectionsTest.java`.

- **ExpressionBuilderTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/mathex/ExpressionBuilderTest.java`.

- **ExpressionTest**: Located at `wayang-commons/wayang-core/src/test/java/org/apache/wayang/core/util/mathex/ExpressionTest.java`.

- **ProfileDB**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/ProfileDB.java`.

- **StopWatch**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/instrumentation/StopWatch.java`.

- **MeasurementDeserializer**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/json/MeasurementDeserializer.java`.

- **MeasurementSerializer**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/json/MeasurementSerializer.java`.

- **Experiment**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/model/Experiment.java`.

- **Measurement**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/model/Measurement.java`.

- **Subject**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/model/Subject.java`.

- **Type**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/model/Type.java`.

- **TimeMeasurement**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/model/measurement/TimeMeasurement.java`.

- **FileStorage**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/storage/FileStorage.java`.

- **JDBCStorage**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/storage/JDBCStorage.java`.

- **Storage**: Located at `wayang-commons/wayang-utils-profile-db/src/main/java/org/apache/wayang/commons/util/profiledb/storage/Storage.java`.

- **ProfileDBTest**: Located at `wayang-commons/wayang-utils-profile-db/src/test/java/org/apache/wayang/commons/util/profiledb/ProfileDBTest.java`.

- **TestMemoryMeasurement**: Located at `wayang-commons/wayang-utils-profile-db/src/test/java/org/apache/wayang/commons/util/profiledb/measurement/TestMemoryMeasurement.java`.

- **TestTimeMeasurement**: Located at `wayang-commons/wayang-utils-profile-db/src/test/java/org/apache/wayang/commons/util/profiledb/measurement/TestTimeMeasurement.java`.



## wayang-docs

No main Java classes identified.



## wayang-platforms

### Java Classes:

- **Flink**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/Flink.java`.

- **ChannelConversions**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/channels/ChannelConversions.java`.

- **DataSetChannel**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/channels/DataSetChannel.java`.

- **FlinkCoGroupFunction**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/FlinkCoGroupFunction.java`.

- **FunctionCompiler**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/FunctionCompiler.java`.

- **KeySelectorDistinct**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/KeySelectorDistinct.java`.

- **KeySelectorFunction**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/KeySelectorFunction.java`.

- **OutputFormatConsumer**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/OutputFormatConsumer.java`.

- **WayangFileOutputFormat**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/WayangFileOutputFormat.java`.

- **DummyFilter**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/DummyFilter.java`.

- **DummyMap**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/DummyMap.java`.

- **WayangAggregator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/WayangAggregator.java`.

- **WayangConvergenceCriterion**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/WayangConvergenceCriterion.java`.

- **WayangFilterCriterion**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/WayangFilterCriterion.java`.

- **WayangListValue**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/WayangListValue.java`.

- **WayangValue**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/compiler/criterion/WayangValue.java`.

- **FlinkContextReference**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/execution/FlinkContextReference.java`.

- **FlinkExecutionContext**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/execution/FlinkExecutionContext.java`.

- **FlinkExecutor**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/execution/FlinkExecutor.java`.

- **CartesianMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/CartesianMapping.java`.

- **CoGroupMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/CoGroupMapping.java`.

- **CollectionSourceMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/CollectionSourceMapping.java`.

- **CountMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/CountMapping.java`.

- **DistinctMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/DistinctMapping.java`.

- **DoWhileMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/DoWhileMapping.java`.

- **FilterMapping**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/mapping/FilterMapping.java`.

- **FlatMapMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/FlatMapMapping.java`.

- **GlobalMaterializedGroupMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/GlobalMaterializedGroupMapping.java`.

- **GlobalReduceMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/GlobalReduceMapping.java`.

- **GroupByMapping**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/mapping/GroupByMapping.java`.

- **IntersectMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/IntersectMapping.java`.

- **JoinMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/JoinMapping.java`.

- **LocalCallbackSinkMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/LocalCallbackSinkMapping.java`.

- **LoopMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/LoopMapping.java`.

- **MapMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/MapMapping.java`.

- **MapPartitionsMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/MapPartitionsMapping.java`.

- **Mappings**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/mapping/Mappings.java`.

- **MaterializedGroupByMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/MaterializedGroupByMapping.java`.

- **ObjectFileSinkMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/ObjectFileSinkMapping.java`.

- **ObjectFileSourceMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/ObjectFileSourceMapping.java`.

- **PageRankMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/graph/PageRankMapping.java`.

- **ReduceByMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/ReduceByMapping.java`.

- **RepeatMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/RepeatMapping.java`.

- **SampleMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/SampleMapping.java`.

- **SortMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/SortMapping.java`.

- **TextFileSinkMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/TextFileSinkMapping.java`.

- **TextFileSourceMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/TextFileSourceMapping.java`.

- **UnionAllMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/UnionAllMapping.java`.

- **ZipWithIdMapping**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/ZipWithIdMapping.java`.

- **FlinkCartesianOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkCartesianOperator.java`.

- **FlinkCoGroupOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkCoGroupOperator.java`.

- **FlinkCollectionSink**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkCollectionSink.java`.

- **FlinkCollectionSource**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkCollectionSource.java`.

- **FlinkCountOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkCountOperator.java`.

- **FlinkDistinctOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkDistinctOperator.java`.

- **FlinkDoWhileOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkDoWhileOperator.java`.

- **FlinkExecutionOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkExecutionOperator.java`.

- **FlinkFilterOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkFilterOperator.java`.

- **FlinkFlatMapOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkFlatMapOperator.java`.

- **FlinkGlobalMaterializedGroupOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkGlobalMaterializedGroupOperator.java`.

- **FlinkGlobalReduceOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkGlobalReduceOperator.java`.

- **FlinkGroupByOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkGroupByOperator.java`.

- **FlinkIntersectOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkIntersectOperator.java`.

- **FlinkJoinOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkJoinOperator.java`.

- **FlinkLocalCallbackSink**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkLocalCallbackSink.java`.

- **FlinkLoopOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkLoopOperator.java`.

- **FlinkMapOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkMapOperator.java`.

- **FlinkMapPartitionsOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkMapPartitionsOperator.java`.

- **FlinkMaterializedGroupByOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkMaterializedGroupByOperator.java`.

- **FlinkObjectFileSink**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkObjectFileSink.java`.

- **FlinkObjectFileSource**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkObjectFileSource.java`.

- **FlinkPageRankOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkPageRankOperator.java`.

- **FlinkReduceByOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkReduceByOperator.java`.

- **FlinkRepeatExpandedOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkRepeatExpandedOperator.java`.

- **FlinkRepeatOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkRepeatOperator.java`.

- **FlinkSampleOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkSampleOperator.java`.

- **FlinkSortOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkSortOperator.java`.

- **FlinkTextFileSink**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkTextFileSink.java`.

- **FlinkTextFileSource**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkTextFileSource.java`.

- **FlinkTsvFileSink**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkTsvFileSink.java`.

- **FlinkUnionAllOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkUnionAllOperator.java`.

- **FlinkZipWithIdOperator**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/operators/FlinkZipWithIdOperator.java`.

- **FlinkPlatform**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/platform/FlinkPlatform.java`.

- **FlinkBasicPlugin**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/plugin/FlinkBasicPlugin.java`.

- **FlinkConversionPlugin**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/plugin/FlinkConversionPlugin.java`.

- **FlinkGraphPlugin**: Located at `wayang-platforms/wayang-flink/code/main/java/org/apache/wayang/flink/plugin/FlinkGraphPlugin.java`.

- **FlinkCartesianOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkCartesianOperatorTest.java`.

- **FlinkCoGroupOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkCoGroupOperatorTest.java`.

- **FlinkCollectionSourceTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkCollectionSourceTest.java`.

- **FlinkCountOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkCountOperatorTest.java`.

- **FlinkDistinctOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkDistinctOperatorTest.java`.

- **FlinkFilterOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkFilterOperatorTest.java`.

- **FlinkFlatMapOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkFlatMapOperatorTest.java`.

- **FlinkGlobalMaterializedGroupOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkGlobalMaterializedGroupOperatorTest.java`.

- **FlinkGlobalReduceOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkGlobalReduceOperatorTest.java`.

- **FlinkJoinOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkJoinOperatorTest.java`.

- **FlinkMapPartitionsOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkMapPartitionsOperatorTest.java`.

- **FlinkMaterializedGroupByOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkMaterializedGroupByOperatorTest.java`.

- **FlinkOperatorTestBase**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkOperatorTestBase.java`.

- **FlinkReduceByOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkReduceByOperatorTest.java`.

- **FlinkSortOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkSortOperatorTest.java`.

- **FlinkUnionAllOperatorTest**: Located at `wayang-platforms/wayang-flink/code/test/java/org/apache/wayang/flink/operators/FlinkUnionAllOperatorTest.java`.

- **ChannelFactory**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/test/ChannelFactory.java`.

- **Giraph**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/Giraph.java`.

- **PageRankAlgorithm**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/Algorithm/PageRankAlgorithm.java`.

- **PageRankParameters**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/Algorithm/PageRankParameters.java`.

- **GiraphExecutor**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/execution/GiraphExecutor.java`.

- **GiraphExecutionOperator**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/operators/GiraphExecutionOperator.java`.

- **GiraphPageRankOperator**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/operators/GiraphPageRankOperator.java`.

- **GiraphPlatform**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/platform/GiraphPlatform.java`.

- **GiraphPlugin**: Located at `wayang-platforms/wayang-giraph/src/main/java/org/apache/wayang/giraph/plugin/GiraphPlugin.java`.

- **GiraphPagaRankOperatorTest**: Located at `wayang-platforms/wayang-giraph/src/test/java/org/apache/wayang/giraph/operators/GiraphPagaRankOperatorTest.java`.

- **GraphChi**: Located at `wayang-platforms/wayang-graphchi/src/main/java/org/apache/wayang/graphchi/GraphChi.java`.

- **GraphChiExecutor**: Located at `wayang-platforms/wayang-graphchi/src/main/java/org/apache/wayang/graphchi/execution/GraphChiExecutor.java`.

- **GraphChiExecutionOperator**: Located at `wayang-platforms/wayang-graphchi/src/main/java/org/apache/wayang/graphchi/operators/GraphChiExecutionOperator.java`.

- **GraphChiPageRankOperator**: Located at `wayang-platforms/wayang-graphchi/src/main/java/org/apache/wayang/graphchi/operators/GraphChiPageRankOperator.java`.

- **GraphChiPlatform**: Located at `wayang-platforms/wayang-graphchi/src/main/java/org/apache/wayang/graphchi/platform/GraphChiPlatform.java`.

- **GraphChiPlugin**: Located at `wayang-platforms/wayang-graphchi/src/main/java/org/apache/wayang/graphchi/plugin/GraphChiPlugin.java`.

- **GraphChiPageRankOperatorTest**: Located at `wayang-platforms/wayang-graphchi/src/test/java/org/apache/wayang/graphchi/operators/GraphChiPageRankOperatorTest.java`.

- **Java**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/Java.java`.

- **CollectionChannel**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/channels/CollectionChannel.java`.

- **JavaChannelInstance**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/channels/JavaChannelInstance.java`.

- **StreamChannel**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/channels/StreamChannel.java`.

- **JavaExecutionContext**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/execution/JavaExecutionContext.java`.

- **JavaExecutor**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/execution/JavaExecutor.java`.

- **JavaCartesianOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaCartesianOperator.java`.

- **JavaCoGroupOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaCoGroupOperator.java`.

- **JavaCollectOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaCollectOperator.java`.

- **JavaCollectionSource**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaCollectionSource.java`.

- **JavaCountOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaCountOperator.java`.

- **JavaDistinctOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaDistinctOperator.java`.

- **JavaDoWhileOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaDoWhileOperator.java`.

- **JavaExecutionOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaExecutionOperator.java`.

- **JavaFilterOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaFilterOperator.java`.

- **JavaFlatMapOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaFlatMapOperator.java`.

- **JavaGlobalMaterializedGroupOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaGlobalMaterializedGroupOperator.java`.

- **JavaGlobalReduceOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaGlobalReduceOperator.java`.

- **JavaIntersectOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaIntersectOperator.java`.

- **JavaJoinOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaJoinOperator.java`.

- **JavaLocalCallbackSink**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaLocalCallbackSink.java`.

- **JavaLoopOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaLoopOperator.java`.

- **JavaMapOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaMapOperator.java`.

- **JavaMapPartitionsOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaMapPartitionsOperator.java`.

- **JavaMaterializedGroupByOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaMaterializedGroupByOperator.java`.

- **JavaObjectFileSink**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaObjectFileSink.java`.

- **JavaObjectFileSource**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaObjectFileSource.java`.

- **JavaRandomSampleOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaRandomSampleOperator.java`.

- **JavaReduceByOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaReduceByOperator.java`.

- **JavaRepeatOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaRepeatOperator.java`.

- **JavaReservoirSampleOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaReservoirSampleOperator.java`.

- **JavaSortOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaSortOperator.java`.

- **JavaTextFileSink**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaTextFileSink.java`.

- **JavaTextFileSource**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaTextFileSource.java`.

- **JavaTsvFileSink**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaTsvFileSink.java`.

- **JavaTsvFileSource**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaTsvFileSource.java`.

- **JavaUnionAllOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/JavaUnionAllOperator.java`.

- **JavaPageRankOperator**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/operators/graph/JavaPageRankOperator.java`.

- **JavaPlatform**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/platform/JavaPlatform.java`.

- **JavaBasicPlugin**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/plugin/JavaBasicPlugin.java`.

- **JavaChannelConversionPlugin**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/plugin/JavaChannelConversionPlugin.java`.

- **JavaGraphPlugin**: Located at `wayang-platforms/wayang-java/src/main/java/org/apache/wayang/java/plugin/JavaGraphPlugin.java`.

- **JavaExecutorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/execution/JavaExecutorTest.java`.

- **JavaCartesianOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaCartesianOperatorTest.java`.

- **JavaCoGroupOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaCoGroupOperatorTest.java`.

- **JavaCollectionSourceTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaCollectionSourceTest.java`.

- **JavaCountOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaCountOperatorTest.java`.

- **JavaDistinctOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaDistinctOperatorTest.java`.

- **JavaExecutionOperatorTestBase**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaExecutionOperatorTestBase.java`.

- **JavaFilterOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaFilterOperatorTest.java`.

- **JavaGlobalMaterializedGroupOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaGlobalMaterializedGroupOperatorTest.java`.

- **JavaGlobalReduceOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaGlobalReduceOperatorTest.java`.

- **JavaJoinOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaJoinOperatorTest.java`.

- **JavaLocalCallbackSinkTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaLocalCallbackSinkTest.java`.

- **JavaMaterializedGroupByOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaMaterializedGroupByOperatorTest.java`.

- **JavaObjectFileSinkTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaObjectFileSinkTest.java`.

- **JavaObjectFileSourceTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaObjectFileSourceTest.java`.

- **JavaRandomSampleOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaRandomSampleOperatorTest.java`.

- **JavaReduceByOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaReduceByOperatorTest.java`.

- **JavaReservoirSampleOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaReservoirSampleOperatorTest.java`.

- **JavaSortOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaSortOperatorTest.java`.

- **JavaTextFileSinkTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaTextFileSinkTest.java`.

- **JavaUnionAllOperatorTest**: Located at `wayang-platforms/wayang-java/src/test/java/org/apache/wayang/java/operators/JavaUnionAllOperatorTest.java`.

- **SqlQueryChannel**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/channels/SqlQueryChannel.java`.

- **DatabaseDescriptor**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/execution/DatabaseDescriptor.java`.

- **JdbcExecutor**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/execution/JdbcExecutor.java`.

- **JdbcExecutionOperator**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/operators/JdbcExecutionOperator.java`.

- **JdbcFilterOperator**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/operators/JdbcFilterOperator.java`.

- **JdbcProjectionOperator**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/operators/JdbcProjectionOperator.java`.

- **JdbcTableSource**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/operators/JdbcTableSource.java`.

- **SqlToStreamOperator**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/operators/SqlToStreamOperator.java`.

- **JdbcPlatformTemplate**: Located at `wayang-platforms/wayang-jdbc-template/src/main/java/org/apache/wayang/jdbc/platform/JdbcPlatformTemplate.java`.

- **JdbcExecutorTest**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/execution/JdbcExecutorTest.java`.

- **JdbcTableSourceTest**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/operators/JdbcTableSourceTest.java`.

- **OperatorTestBase**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/operators/OperatorTestBase.java`.

- **SqlToStreamOperatorTest**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/operators/SqlToStreamOperatorTest.java`.

- **HsqldbFilterOperator**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/test/HsqldbFilterOperator.java`.

- **HsqldbPlatform**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/test/HsqldbPlatform.java`.

- **HsqldbProjectionOperator**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/test/HsqldbProjectionOperator.java`.

- **HsqldbTableSource**: Located at `wayang-platforms/wayang-jdbc-template/src/test/java/org/apache/wayang/jdbc/test/HsqldbTableSource.java`.

- **Postgres**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/Postgres.java`.

- **ProjectionMapping**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/mapping/ProjectionMapping.java`.

- **PostgresExecutionOperator**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/operators/PostgresExecutionOperator.java`.

- **PostgresFilterOperator**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/operators/PostgresFilterOperator.java`.

- **PostgresProjectionOperator**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/operators/PostgresProjectionOperator.java`.

- **PostgresTableSource**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/operators/PostgresTableSource.java`.

- **PostgresPlatform**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/platform/PostgresPlatform.java`.

- **PostgresConversionsPlugin**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/plugin/PostgresConversionsPlugin.java`.

- **PostgresPlugin**: Located at `wayang-platforms/wayang-postgres/src/main/java/org/apache/wayang/postgres/plugin/PostgresPlugin.java`.

- **Spark**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/Spark.java`.

- **BroadcastChannel**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/channels/BroadcastChannel.java`.

- **FileChannels**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/channels/FileChannels.java`.

- **RddChannel**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/channels/RddChannel.java`.

- **BinaryOperatorAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/BinaryOperatorAdapter.java`.

- **ExtendedBinaryOperatorAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/ExtendedBinaryOperatorAdapter.java`.

- **ExtendedFlatMapFunctionAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/ExtendedFlatMapFunctionAdapter.java`.

- **ExtendedFunction**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/ExtendedFunction.java`.

- **ExtendedMapFunctionAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/ExtendedMapFunctionAdapter.java`.

- **ExtendedMapPartitionsFunctionAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/ExtendedMapPartitionsFunctionAdapter.java`.

- **ExtendedPredicateAdapater**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/ExtendedPredicateAdapater.java`.

- **FlatMapFunctionAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/FlatMapFunctionAdapter.java`.

- **MapFunctionAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/MapFunctionAdapter.java`.

- **MapPartitionsFunctionAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/MapPartitionsFunctionAdapter.java`.

- **PredicateAdapter**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/compiler/PredicateAdapter.java`.

- **SparkContextReference**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/execution/SparkContextReference.java`.

- **SparkExecutionContext**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/execution/SparkExecutionContext.java`.

- **SparkExecutor**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/execution/SparkExecutor.java`.

- **SparkBernoulliSampleOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkBernoulliSampleOperator.java`.

- **SparkBroadcastOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkBroadcastOperator.java`.

- **SparkCacheOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkCacheOperator.java`.

- **SparkCartesianOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkCartesianOperator.java`.

- **SparkCoGroupOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkCoGroupOperator.java`.

- **SparkCollectOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkCollectOperator.java`.

- **SparkCollectionSource**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkCollectionSource.java`.

- **SparkCountOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkCountOperator.java`.

- **SparkDistinctOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkDistinctOperator.java`.

- **SparkDoWhileOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkDoWhileOperator.java`.

- **SparkExecutionOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkExecutionOperator.java`.

- **SparkFilterOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkFilterOperator.java`.

- **SparkFlatMapOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkFlatMapOperator.java`.

- **SparkGlobalMaterializedGroupOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkGlobalMaterializedGroupOperator.java`.

- **SparkGlobalReduceOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkGlobalReduceOperator.java`.

- **SparkIntersectOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkIntersectOperator.java`.

- **SparkJoinOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkJoinOperator.java`.

- **SparkLocalCallbackSink**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkLocalCallbackSink.java`.

- **SparkLoopOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkLoopOperator.java`.

- **SparkMapOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkMapOperator.java`.

- **SparkMapPartitionsOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkMapPartitionsOperator.java`.

- **SparkMaterializedGroupByOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkMaterializedGroupByOperator.java`.

- **SparkObjectFileSink**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkObjectFileSink.java`.

- **SparkObjectFileSource**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkObjectFileSource.java`.

- **SparkRandomPartitionSampleOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkRandomPartitionSampleOperator.java`.

- **SparkReduceByOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkReduceByOperator.java`.

- **SparkRepeatOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkRepeatOperator.java`.

- **SparkShufflePartitionSampleOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkShufflePartitionSampleOperator.java`.

- **SparkSortOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkSortOperator.java`.

- **SparkTextFileSink**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkTextFileSink.java`.

- **SparkTextFileSource**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkTextFileSource.java`.

- **SparkTsvFileSink**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkTsvFileSink.java`.

- **SparkTsvFileSource**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkTsvFileSource.java`.

- **SparkUnionAllOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkUnionAllOperator.java`.

- **SparkZipWithIdOperator**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkZipWithIdOperator.java`.

- **SparkPlatform**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/platform/SparkPlatform.java`.

- **SparkBasicPlugin**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/plugin/SparkBasicPlugin.java`.

- **SparkConversionPlugin**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/plugin/SparkConversionPlugin.java`.

- **SparkGraphPlugin**: Located at `wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/plugin/SparkGraphPlugin.java`.

- **SparkBernoulliSampleOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkBernoulliSampleOperatorTest.java`.

- **SparkCartesianOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkCartesianOperatorTest.java`.

- **SparkCoGroupOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkCoGroupOperatorTest.java`.

- **SparkCollectionSourceTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkCollectionSourceTest.java`.

- **SparkCountOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkCountOperatorTest.java`.

- **SparkDistinctOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkDistinctOperatorTest.java`.

- **SparkFilterOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkFilterOperatorTest.java`.

- **SparkFlatMapOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkFlatMapOperatorTest.java`.

- **SparkGlobalMaterializedGroupOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkGlobalMaterializedGroupOperatorTest.java`.

- **SparkGlobalReduceOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkGlobalReduceOperatorTest.java`.

- **SparkJoinOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkJoinOperatorTest.java`.

- **SparkMapPartitionsOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkMapPartitionsOperatorTest.java`.

- **SparkMaterializedGroupByOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkMaterializedGroupByOperatorTest.java`.

- **SparkObjectFileSinkTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkObjectFileSinkTest.java`.

- **SparkObjectFileSourceTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkObjectFileSourceTest.java`.

- **SparkOperatorTestBase**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkOperatorTestBase.java`.

- **SparkRandomPartitionSampleOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkRandomPartitionSampleOperatorTest.java`.

- **SparkReduceByOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkReduceByOperatorTest.java`.

- **SparkShufflePartitionSampleOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkShufflePartitionSampleOperatorTest.java`.

- **SparkSortOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkSortOperatorTest.java`.

- **SparkTextFileSinkTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkTextFileSinkTest.java`.

- **SparkUnionAllOperatorTest**: Located at `wayang-platforms/wayang-spark/code/test/java/org/apache/wayang/spark/operators/SparkUnionAllOperatorTest.java`.

- **Sqlite3**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/Sqlite3.java`.

- **Sqlite3FilterOperator**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/operators/Sqlite3FilterOperator.java`.

- **Sqlite3ProjectionOperator**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/operators/Sqlite3ProjectionOperator.java`.

- **Sqlite3TableSource**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/operators/Sqlite3TableSource.java`.

- **Sqlite3Platform**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/platform/Sqlite3Platform.java`.

- **Sqlite3ConversionPlugin**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/plugin/Sqlite3ConversionPlugin.java`.

- **Sqlite3Plugin**: Located at `wayang-platforms/wayang-sqlite3/src/main/java/org/apache/wayang/sqlite3/plugin/Sqlite3Plugin.java`.



## wayang-plugins

### Java Classes:

- **IEJoin**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/IEJoin.java`.

- **Data**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/data/Data.java`.

- **Mappings**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/mapping/Mappings.java`.

- **IEJoinMapping**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/mapping/spark/IEJoinMapping.java`.

- **IESelfJoinMapping**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/mapping/spark/IESelfJoinMapping.java`.

- **IEJoinMasterOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/IEJoinMasterOperator.java`.

- **IEJoinOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/IEJoinOperator.java`.

- **IESelfJoinOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/IESelfJoinOperator.java`.

- **JavaIEJoinOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/JavaIEJoinOperator.java`.

- **JavaIESelfJoinOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/JavaIESelfJoinOperator.java`.

- **SparkIEJoinOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/SparkIEJoinOperator.java`.

- **SparkIESelfJoinOperator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/SparkIESelfJoinOperator.java`.

- **BitSetJoin**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/BitSetJoin.java`.

- **DataComparator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/java_helpers/DataComparator.java`.

- **extractData**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/extractData.java`.

- **myMergeSort**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/myMergeSort.java`.

- **revDataComparator**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/revDataComparator.java`.

- **List2AttributesObjectSkinny**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/List2AttributesObjectSkinny.java`.

- **addUniqueID**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/addUniqueID.java`.

- **build2ListObject**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/build2ListObject.java`.

- **filterUnwantedBlocks**: Located at `wayang-plugins/wayang-iejoin/code/main/java/org/apache/wayang/iejoin/operators/spark_helpers/filterUnwantedBlocks.java`.

- **JavaExecutionOperatorTestBase**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/JavaExecutionOperatorTestBase.java`.

- **JavaIEJoinOperatorTest**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/JavaIEJoinOperatorTest.java`.

- **SparkIEJoinOperatorTest**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/SparkIEJoinOperatorTest.java`.

- **SparkIEJoinOperatorTest2**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/SparkIEJoinOperatorTest2.java`.

- **SparkIEJoinOperatorTest3**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/SparkIEJoinOperatorTest3.java`.

- **SparkIEJoinOperatorTest4**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/SparkIEJoinOperatorTest4.java`.

- **SparkIESelfJoinOperatorTest**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/SparkIESelfJoinOperatorTest.java`.

- **SparkOperatorTestBase**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/operators/SparkOperatorTestBase.java`.

- **ChannelFactory**: Located at `wayang-plugins/wayang-iejoin/code/test/java/org/apache/wayang/iejoin/test/ChannelFactory.java`.



## wayang-profiler

### Java Classes:

- **DataGenerators**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/data/DataGenerators.java`.

- **DiskProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/hardware/DiskProfiler.java`.

- **BinaryOperatorProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/BinaryOperatorProfiler.java`.

- **JavaCollectionSourceProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/java/JavaCollectionSourceProfiler.java`.

- **JavaTextFileSourceProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/java/JavaTextFileSourceProfiler.java`.

- **OperatorProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/java/OperatorProfiler.java`.

- **OperatorProfilers**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/OperatorProfilers.java`.

- **Profiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/java/Profiler.java`.

- **SinkProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/SinkProfiler.java`.

- **SourceProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/java/SourceProfiler.java`.

- **UnaryOperatorProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/java/UnaryOperatorProfiler.java`.

- **DynamicEstimationContext**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/DynamicEstimationContext.java`.

- **DynamicLoadEstimator**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/DynamicLoadEstimator.java`.

- **DynamicLoadProfileEstimator**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/DynamicLoadProfileEstimator.java`.

- **DynamicLoadProfileEstimators**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/DynamicLoadProfileEstimators.java`.

- **GeneticOptimizer**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/GeneticOptimizer.java`.

- **GeneticOptimizerApp**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/GeneticOptimizerApp.java`.

- **Individual**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/Individual.java`.

- **LogEvaluator**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/LogEvaluator.java`.

- **OptimizationSpace**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/OptimizationSpace.java`.

- **Variable**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/Variable.java`.

- **Battle**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/sampling/Battle.java`.

- **ReservoirSampler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/sampling/ReservoirSampler.java`.

- **Sampler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/sampling/Sampler.java`.

- **TournamentSampler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/log/sampling/TournamentSampler.java`.

- **Main**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/Main.java`.

- **SparkCollectionSourceProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/SparkCollectionSourceProfiler.java`.

- **SparkOperatorProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/SparkOperatorProfiler.java`.

- **SparkSourceProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/SparkSourceProfiler.java`.

- **SparkTextFileSourceProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/SparkTextFileSourceProfiler.java`.

- **SparkUnaryOperatorProfiler**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/spark/SparkUnaryOperatorProfiler.java`.

- **ProfilingUtils**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/util/ProfilingUtils.java`.

- **RrdAccessor**: Located at `wayang-profiler/code/main/java/org/apache/wayang/profiler/util/RrdAccessor.java`.



## wayang-resources

No main Java classes identified.



## wayang-tests-integration

### Java Classes:

- **FlinkIntegrationIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/FlinkIntegrationIT.java`.

- **FullIntegrationIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/FullIntegrationIT.java`.

- **GiraphIntegrationIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/GiraphIntegrationIT.java`.

- **JavaIntegrationIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/JavaIntegrationIT.java`.

- **PostgresIntegrationIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/PostgresIntegrationIT.java`.

- **RegressionIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/RegressionIT.java`.

- **SparkIntegrationIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/SparkIntegrationIT.java`.

- **WayangPlans**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/WayangPlans.java`.

- **WayangPlansOperators**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/WayangPlansOperators.java`.

- **WordCountIT**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/WordCountIT.java`.

- **MyMadeUpPlatform**: Located at `wayang-tests-integration/code/test/java/org/apache/wayang/tests/platform/MyMadeUpPlatform.java`.

- **GraphChiIntegrationIT**: Located at `wayang-tests-integration/wayang-tests-integration_2.11/src/test/java/org/apache/wayang/test/GraphChiIntegrationIT.java`.


## wayang-flink



## wayang-flink
