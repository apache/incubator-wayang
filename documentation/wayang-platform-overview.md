# .github

# .mvn

# bin

# build

# conf

# guides

## WordCount

---

# images

# python

# src

# tools

# wayang-api

## PythonAPI

### Public Methods:

- **empty()**: Returns `void`

---

## ProcessFeeder

### Fields:

- **socket**: `Socket` (private)
- **serializedUDF**: `PythonCode` (private)
- **input**: `Iterable<Input>` (private)

### Public Methods:

- **send()**: Returns `void`
- **writeUDF()**: Returns `void`
- **writeIteratorToStream()**: Returns `void`
- **write()**: Returns `void`
- **writeBytes()**: Returns `void`
- **writeUTF()**: Returns `void`
- **writeKeyValue()**: Returns `void`

---

## ProcessReceiver

### Fields:

- **iterator**: `ReaderIterator<Output>` (private)

### Public Methods:

- **getIterable()**: Returns `Iterable<Output>`
- **print()**: Returns `void`

---

## PythonProcessCaller

### Fields:

- **process**: `Thread` (private)
- **socket**: `Socket` (private)
- **serverSocket**: `ServerSocket` (private)
- **ready**: `boolean` (private)
- **configuration**: `Configuration` (private)

### Public Methods:

- **getProcess()**: Returns `Thread`
- **getSocket()**: Returns `Socket`
- **isReady()**: Returns `boolean`
- **close()**: Returns `void`

---

## PythonWorkerManager

### Fields:

- **serializedUDF**: `PythonCode` (private)
- **inputIterator**: `Iterable<Input>` (private)

### Public Methods:

- **execute()**: Returns `Iterable<Output>`

---

## ReaderIterator
- **Implements**: Iterator

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `Output`

### Private Methods:

- **read()**: Returns `Output`

---

## PythonCode
- **Implements**: Serializable

---

## PythonFunctionWrapper
- **Implements**: FunctionDescriptor

### Fields:

- **serializedUDF**: `PythonCode` (private)

### Public Methods:

- **apply()**: Returns `Iterable<Output>`

---

## PythonUDF
- **Extends**: FunctionDescriptor

---

## PythonAPITest

---

## WayangPlanBuilder

### Fields:

- **wayangPlan**: `WayangPlan` (private)
- **wayangContext**: `WayangContext` (private)

### Public Methods:

- **createOperatorByType()**: Returns `OperatorBase`
- **getWayangContext()**: Returns `WayangContext`
- **getWayangPlan()**: Returns `WayangPlan`

### Private Methods:

- **buildContext()**: Returns `WayangContext`
- **buildPlan()**: Returns `WayangPlan`

---

## WayangController

### Public Methods:

- **planFromFile()**: Returns `String`
- **planFromMessage()**: Returns `String`
- **all()**: Returns `String`
- **createOperatorByType()**: Returns `OperatorBase`

### Private Methods:

- **buildContext()**: Returns `WayangContext`
- **buildPlan()**: Returns `WayangPlan`

---

## JavaApiTest

**Description**: * Test suite for the Java API.

- **Implements**: FunctionDescriptor

### Fields:

- **sqlite3Configuration**: `Configuration` (private)
- **offset**: `int` (private)
- **selectors**: `Collection<Character>` (private)

### Public Methods:

- **setUp()**: Returns `void`
- **testMapReduce()**: Returns `void`
- **testMapReduceBy()**: Returns `void`
- **testBroadcast2()**: Returns `void`
- **testCustomOperatorShortCut()**: Returns `void`
- **testWordCount()**: Returns `void`
- **testWordCountOnSparkAndJava()**: Returns `void`
- **testSample()**: Returns `void`
- **testDoWhile()**: Returns `void`
- **open()**: Returns `void`
- **apply()**: Returns `Integer`
- **testRepeat()**: Returns `void`
- **open()**: Returns `void`
- **test()**: Returns `boolean`
- **testBroadcast()**: Returns `void`
- **testGroupBy()**: Returns `void`
- **testJoin()**: Returns `void`
- **testJoinAndAssemble()**: Returns `void`
- **testCoGroup()**: Returns `void`
- **testCoGroupViaKeyBy()**: Returns `void`
- **testIntersect()**: Returns `void`
- **testSort()**: Returns `void`
- **testPageRank()**: Returns `void`
- **testMapPartitions()**: Returns `void`
- **testZipWithId()**: Returns `void`
- **testWriteTextFile()**: Returns `void`
- **testSqlOnJava()**: Returns `void`
- **testSqlOnSqlite3()**: Returns `void`

---

## WayangConvention
- **Implements**: Convention

### Public Methods:

- **getInterface()**: Returns `Class`
- **getName()**: Returns `String`
- **canConvertConvention()**: Returns `boolean`
- **useAbstractConvertersForConversion()**: Returns `boolean`
- **getTraitDef()**: Returns `RelTraitDef`
- **satisfies()**: Returns `boolean`
- **register()**: Returns `void`
- **toString()**: Returns `String`

---

## WayangFilterVisitor
- **Extends**: WayangRelNodeVisitor
- **Implements**: FunctionDescriptor

### Public Methods:

- **test()**: Returns `boolean`
- **visitCall()**: Returns `Boolean`
- **eval()**: Returns `boolean`

### Private Methods:

- **isGreaterThan()**: Returns `boolean`
- **isLessThan()**: Returns `boolean`
- **isEqualTo()**: Returns `boolean`

---

## WayangJoinVisitor
- **Extends**: WayangRelNodeVisitor
- **Implements**: FunctionDescriptor

### Public Methods:

- **visitCall()**: Returns `Integer`
- **apply()**: Returns `Object`
- **apply()**: Returns `Record`

---

## WayangProjectVisitor
- **Extends**: WayangRelNodeVisitor

### Public Methods:

- **apply()**: Returns `Record`

---

## WayangRelConverter

### Public Methods:

- **convert()**: Returns `Operator`

---

## WayangRelNodeVisitor
- **Extends**: RelNode

---

## WayangTableScanVisitor
- **Extends**: WayangRelNodeVisitor

---

## JdbcSchema

**Description**: * Implementation of {@link Schema} that is backed by a JDBC data source.
 *
 * <p>The tables in the JDBC data source appear to be tables in this schema;
 * queries against this schema are executed against those tables, pushing down
 * as much as possible of the query logic to SQL.</p>

- **Extends**: BiFunction
- **Implements**: Schema

### Public Methods:

- **isMutable()**: Returns `boolean`
- **snapshot()**: Returns `Schema`
- **getDataSource()**: Returns `DataSource`
- **getExpression()**: Returns `Expression`
- **getTableNames()**: Returns `Set<String>`
- **getTypeNames()**: Returns `Set<String>`
- **getSubSchemaNames()**: Returns `Set<String>`
- **create()**: Returns `Schema`

---

## JdbcTable

**Description**: * Queryable that gets its data from a table within a JDBC connection.
 *
 * <p>The idea is not to read the whole table, however. The idea is to use
 * this as a building block for a query, by applying Queryable operators
 * such as
 * {@link Queryable#where(org.apache.calcite.linq4j.function.Predicate2)}.
 * The resulting queryable can then be converted to a SQL query, which can be
 * executed efficiently on the JDBC server.</p>

- **Extends**: AbstractQueryableTable
- **Implements**: TranslatableTable, ScannableTable, ModifiableTable

### Public Methods:

- **toString()**: Returns `String`
- **getRowType()**: Returns `RelDataType`
- **tableName()**: Returns `SqlIdentifier`
- **toRel()**: Returns `RelNode`
- **toModificationRel()**: Returns `TableModify`
- **toString()**: Returns `String`
- **enumerator()**: Returns `Enumerator<T>`

### Private Methods:

- **supplyProto()**: Returns `RelProtoDataType`

---

## JdbcUtils
- **Extends**: ObjectArrayRowBuilder
- **Implements**: Function0

### Public Methods:

- **get()**: Returns `SqlDialect`
- **get()**: Returns `DataSource`

---

## Optimizer

### Public Methods:

- **parseSql()**: Returns `SqlNode`
- **validate()**: Returns `SqlNode`
- **convert()**: Returns `RelNode`
- **optimize()**: Returns `RelNode`
- **convert()**: Returns `WayangPlan`
- **convert()**: Returns `WayangPlan`

---

## WayangProgram
- **Implements**: Program

### Public Methods:

- **run()**: Returns `RelNode`

---

## WayangFilter
- **Extends**: Filter
- **Implements**: WayangRel

### Public Methods:

- **copy()**: Returns `Filter`
- **toString()**: Returns `String`

---

## WayangJoin
- **Extends**: Join
- **Implements**: WayangRel

### Public Methods:

- **copy()**: Returns `WayangJoin`
- **toString()**: Returns `String`

---

## WayangProject
- **Extends**: Project
- **Implements**: WayangRel

### Public Methods:

- **copy()**: Returns `WayangProject`
- **toString()**: Returns `String`

---

## WayangRel
- **Extends**: RelNode

---

## WayangTableScan
- **Extends**: TableScan
- **Implements**: WayangRel

### Public Methods:

- **toString()**: Returns `String`
- **getQualifiedName()**: Returns `String`
- **getTableName()**: Returns `String`
- **getColumnNames()**: Returns `List<String>`

---

## WayangRules
- **Extends**: ConverterRule

### Public Methods:

- **convert()**: Returns `RelNode`
- **convert()**: Returns `RelNode`

---

## SchemaUtils

---

## WayangSchema
- **Extends**: AbstractSchema

### Public Methods:

- **getSchemaName()**: Returns `String`
- **snapshot()**: Returns `Schema`

---

## WayangSchemaBuilder

### Public Methods:

- **addTable()**: Returns `WayangSchemaBuilder`
- **build()**: Returns `WayangSchema`

---

## WayangTable
- **Extends**: AbstractTable

### Fields:

- **rowType**: `RelDataType` (private)

### Public Methods:

- **getTableName()**: Returns `String`
- **getRowType()**: Returns `RelDataType`
- **getStatistic()**: Returns `Statistic`

---

## WayangTableBuilder

### Fields:

- **rowCount**: `long` (private)

### Public Methods:

- **addField()**: Returns `WayangTableBuilder`
- **withRowCount()**: Returns `WayangTableBuilder`
- **build()**: Returns `WayangTable`

---

## WayangTableStatistic
- **Implements**: Statistic

### Public Methods:

- **isKey()**: Returns `boolean`

---

## ModelParser

### Fields:

- **configuration**: `Configuration` (private)
- **json**: `JSONObject` (private)

### Public Methods:

- **setProperties()**: Returns `Configuration`
- **getFsPath()**: Returns `String`
- **getSeparator()**: Returns `String`

---

## PrintUtils

---

## SqlContext

### Public Methods:

- **executeSql()**: Returns `Collection<Record>`

---

## CsvRowConverter

**Description**: * Based on Calcite's CSV enumerator.
 * TODO: handle different variants
 *


---

## JavaCSVTableSource
- **Extends**: UnarySource
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `String`

### Private Methods:

- **createStream()**: Returns `Stream<T>`
- **parseLine()**: Returns `T`
- **advance()**: Returns `void`

---

## SqlAPI

---

## SqlTest

---

## SqlToWayangRelTest

### Public Methods:

- **test_simple_sql()**: Returns `void`

### Private Methods:

- **print()**: Returns `void`
- **print()**: Returns `void`

---

# wayang-assembly

# wayang-benchmark

## Grep
- **Implements**: Serializable

### Public Methods:

- **iterator()**: Returns `Iterator<CharSequence>`

---

## SGDImpl

**Description**: * This class executes a stochastic gradient descent optimization on Apache Wayang (incubating).

- **Implements**: FunctionDescriptor

### Fields:

- **accuracy**: `double` (public)
- **max_iterations**: `int` (public)
- **current_iteration**: `int` (private)

### Public Methods:

- **open()**: Returns `void`
- **open()**: Returns `void`
- **open()**: Returns `void`
- **test()**: Returns `boolean`
- **open()**: Returns `void`

---

## SGDImprovedImpl

**Description**: * This class executes a stochastic gradient descent optimization on Apache Wayang (incubating), just like {@link SGDImpl}. However,
 * it used the {@link org.apache.wayang.basic.operators.MapPartitionsOperator} for performance improvements.

- **Implements**: FunctionDescriptor

### Public Methods:

- **open()**: Returns `void`

---

## Random16
- **Implements**: a 128

---

## Unsigned16
- **Implements**: Writable, Serializable

### Fields:

- **hi8**: `long` (private)
- **lo8**: `long` (private)

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **set()**: Returns `void`
- **set()**: Returns `void`
- **toString()**: Returns `String`
- **getByte()**: Returns `byte`
- **getHexDigit()**: Returns `char`
- **getHigh8()**: Returns `long`
- **getLow8()**: Returns `long`
- **add()**: Returns `void`
- **shiftLeft()**: Returns `void`
- **readFields()**: Returns `void`
- **write()**: Returns `void`

---

## Main

**Description**: * Example Apache Wayang (incubating) App that does a word count -- the Hello World of Map/Reduce-like systems.


---

## LineItemTuple

**Description**: * A tuple of the lineitem table.
 * <p>Example line:</p>
 * <pre>
 * "3249925";"37271";"9775";"1";"9.00";"10874.43";"0.10";"0.04";"N";"O";"1998-04-19";"1998-06-17";"1998-04-21";"TAKE BACK RETURN         ";"AIR       ";"express instructions among the excuses nag"
 * </pre>

- **Implements**: Serializable

### Fields:

- **L_ORDERKEY**: `long` (public)
- **L_PARTKEY**: `long` (public)
- **L_SUPPKEY**: `long` (public)
- **L_LINENUMBER**: `int` (public)
- **L_QUANTITY**: `double` (public)
- **L_EXTENDEDPRICE**: `double` (public)
- **L_DISCOUNT**: `double` (public)
- **L_TAX**: `double` (public)
- **L_RETURNFLAG**: `char` (public)
- **L_LINESTATUS**: `char` (public)
- **L_SHIPDATE**: `int` (public)
- **L_COMMITDATE**: `int` (public)
- **L_RECEIPTDATE**: `int` (public)
- **L_SHIPINSTRUCT**: `String` (public)
- **L_SHIPMODE**: `String` (public)
- **L_COMMENT**: `String` (public)

### Public Methods:

- **parse()**: Returns `LineItemTuple`

---

## GroupKey

**Description**: * Grouping key used in Query 1.

- **Implements**: Serializable

### Fields:

- **L_RETURNFLAG**: `char` (public)
- **L_LINESTATUS**: `char` (public)

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

## ReturnTuple

**Description**: * Tuple that is returned by Query 1.

- **Implements**: Serializable

### Fields:

- **L_RETURNFLAG**: `char` (public)
- **L_LINESTATUS**: `char` (public)
- **SUM_QTY**: `double` (public)
- **SUM_BASE_PRICE**: `double` (public)
- **SUM_DISC_PRICE**: `double` (public)
- **SUM_CHARGE**: `double` (public)
- **AVG_QTY**: `double` (public)
- **AVG_PRICE**: `double` (public)
- **AVG_DISC**: `double` (public)
- **COUNT_ORDER**: `int` (public)

### Public Methods:

- **toString()**: Returns `String`

---

## LineItemTupleTest

**Description**: * Test suited for {@link LineItemTuple}.


### Public Methods:

- **testParser()**: Returns `void`

### Private Methods:

- **toDateInteger()**: Returns `int`

---

# wayang-commons

## WayangBasics

**Description**: * Register for plugins in the module.


---

## FileChannel

**Description**: * Represents a {@link Channel} that is realized via a file/set of files.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `FileChannel`
- **toString()**: Returns `String`
- **createInstance()**: Returns `ChannelInstance`
- **getLocation()**: Returns `String`
- **getSerialization()**: Returns `String`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **getChannel()**: Returns `FileChannel`
- **addPath()**: Returns `void`
- **addGivenOrTempPath()**: Returns `String`
- **getPaths()**: Returns `Collection<String>`
- **getSinglePath()**: Returns `String`
- **doDispose()**: Returns `void`

---

## Record

**Description**: * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.

- **Implements**: Serializable, Copyable

### Public Methods:

- **copy()**: Returns `Record`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField()**: Returns `Object`
- **getDouble()**: Returns `double`
- **getLong()**: Returns `long`
- **getInt()**: Returns `int`
- **getString()**: Returns `String`
- **size()**: Returns `int`

---

## Tuple2

**Description**: * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.

- **Implements**: Serializable

### Fields:

- **field0**: `T0` (public)
- **field1**: `T1` (public)

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **getField1()**: Returns `T1`

---

## Tuple5

**Description**: * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.

- **Implements**: Serializable

### Fields:

- **field0**: `T0` (public)
- **field1**: `T1` (public)
- **field2**: `T2` (public)
- **field3**: `T3` (public)
- **field4**: `T4` (public)

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **getField1()**: Returns `T1`
- **getField2()**: Returns `T2`
- **getField3()**: Returns `T3`
- **getField4()**: Returns `T4`

---

## ProjectionDescriptor

**Description**: * This descriptor pertains to projections. It takes field names of the input type to describe the projection.

- **Extends**: TransformationDescriptor
- **Implements**: FunctionDescriptor

### Fields:

- **fieldNames**: `List<String>` (private)
- **field**: `Field` (private)

### Public Methods:

- **getFieldNames()**: Returns `List<String>`
- **apply()**: Returns `Output`
- **apply()**: Returns `Record`

---

## GlobalReduceMapping

**Description**: * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
 * a single {@link ReduceByOperator}.

- **Extends**: ReplacementSubplanFactory
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`

---

## Mappings

**Description**: * Register for the components provided in the basic plugin.


---

## MaterializedGroupByMapping

**Description**: * This mapping translates the {@link GroupByOperator} into the {@link MaterializedGroupByOperator}.

- **Extends**: ReplacementSubplanFactory
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`

---

## PageRankMapping

**Description**: * This {@link Mapping} translates a {@link PageRankOperator} into a {@link Subplan} of basic {@link Operator}s.

- **Implements**: Mapping

### Fields:

- **initialRank**: `Float` (private)
- **minRank**: `float` (private)

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **open()**: Returns `void`
- **open()**: Returns `void`

### Private Methods:

- **createTransformation()**: Returns `PlanTransformation`
- **createPattern()**: Returns `SubplanPattern`
- **createReplacementFactory()**: Returns `ReplacementSubplanFactory`
- **createPageRankSubplan()**: Returns `Operator`

---

## ReduceByMapping

**Description**: * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
 * a single {@link ReduceByOperator}.

- **Extends**: ReplacementSubplanFactory
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`

---

## RepeatMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createTransformation()**: Returns `PlanTransformation`
- **createPattern()**: Returns `SubplanPattern`
- **createReplacementFactory()**: Returns `ReplacementSubplanFactory`
- **createLoopOperatorSubplan()**: Returns `Subplan`

---

## CartesianOperator

**Description**: * This operator returns the cartesian product of elements of input datasets.

- **Extends**: BinaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## CoGroupOperator

**Description**: * This operator groups both inputs by some key and then matches groups with the same key. If a key appears in only
 * one of the input datasets, then the according group is matched with an empty group.

- **Extends**: BinaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## CollectionSource

**Description**: * This source takes as input a Java {@link java.util.Collection}.

- **Extends**: UnarySource
- **Implements**: ElementaryOperator

### Public Methods:

- **getCollection()**: Returns `Collection<T>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## CountOperator

**Description**: * This operator returns the count of elements in this stream.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## DistinctOperator

**Description**: * This operator returns the distinct elements in this dataset.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## DoWhileOperator

**Description**: * This operator has three inputs and two outputs.

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator, LoopHeadOperator

### Fields:

- **state**: `State` (private)

### Public Methods:

- **getState()**: Returns `State`
- **setState()**: Returns `void`
- **getInputType()**: Returns `DataSetType<InputType>`
- **getConvergenceType()**: Returns `DataSetType<ConvergenceType>`
- **initialize()**: Returns `void`
- **beginIteration()**: Returns `void`
- **endIteration()**: Returns `void`
- **outputConnectTo()**: Returns `void`
- **getCriterionDescriptor()**: Returns `PredicateDescriptor<Collection<ConvergenceType>>`
- **isReading()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **setNumExpectedIterations()**: Returns `void`
- **getNumExpectedIterations()**: Returns `int`

### Private Methods:

- **initializeSlots()**: Returns `void`

---

## FilterOperator

**Description**: * This operator returns a new dataset after filtering by applying predicateDescriptor.

- **Extends**: UnaryToUnaryOperator
- **Implements**: org

### Public Methods:

- **getPredicateDescriptor()**: Returns `PredicateDescriptor<Type>`
- **getType()**: Returns `DataSetType<Type>`
- **estimate()**: Returns `CardinalityEstimate`

---

## FlatMapOperator

**Description**: * A flatmap operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, returning zero or more output elements,
 * flattening the result and pushes it to the output slot.

- **Extends**: UnaryToUnaryOperator
- **Implements**: org

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## GlobalMaterializedGroupOperator

**Description**: * This operator groups the elements of a data set into a single data quantum.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## GlobalReduceOperator

**Description**: * This operator groups the elements of a data set and aggregates the groups.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## GroupByOperator

**Description**: * This is the auxiliary GroupBy operator, i.e., it behaves differently depending on its context. If it is followed
 * by a {@link ReduceOperator} (and akin), it turns that one into a {@link ReduceByOperator}. Otherwise, it corresponds to a
 * {@link MaterializedGroupByOperator}.
 *
 * @see MaterializedGroupByOperator
 * @see ReduceOperator

- **Extends**: UnaryToUnaryOperator

---

## IntersectOperator

**Description**: * This operator returns the set intersection of elements of input datasets.

- **Extends**: BinaryToUnaryOperator

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## JoinOperator

**Description**: * This operator returns the cartesian product of elements of input datasets.

- **Extends**: BinaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## LocalCallbackSink

**Description**: * This sink executes a callback on each received data unit into a Java {@link Collection}.

- **Extends**: UnarySink

### Public Methods:

- **setCollector()**: Returns `LocalCallbackSink<T>`
- **getCallback()**: Returns `Consumer<T>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## LoopOperator

**Description**: * This operator has three inputs and three outputs.

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator, LoopHeadOperator

### Fields:

- **state**: `State` (private)

### Public Methods:

- **getState()**: Returns `State`
- **setState()**: Returns `void`
- **getInputType()**: Returns `DataSetType<InputType>`
- **getConvergenceType()**: Returns `DataSetType<ConvergenceType>`
- **initialize()**: Returns `void`
- **initialize()**: Returns `void`
- **beginIteration()**: Returns `void`
- **beginIteration()**: Returns `void`
- **endIteration()**: Returns `void`
- **endIteration()**: Returns `void`
- **outputConnectTo()**: Returns `void`
- **outputConnectTo()**: Returns `void`
- **getCriterionDescriptor()**: Returns `PredicateDescriptor<Collection<ConvergenceType>>`
- **isReading()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **setNumExpectedIterations()**: Returns `void`
- **getNumExpectedIterations()**: Returns `int`

### Private Methods:

- **initializeSlots()**: Returns `void`

---

## MapOperator

**Description**: * A map operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, and pushes that element to the output slot.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## MapPartitionsOperator

**Description**: * This operator takes as input potentially multiple input data quanta and outputs multiple input data quanta.
 * <p>Since Wayang is not a physical execution engine, its notion of partitions is rather loose. Implementors
 * of this operator should guarantee that the partitions are distinct in their data quanta and that all partitions together
 * are complete w.r.t. the data quanta.</p>
 * <p>However, no further assumptions on partitions shall be made, such as: whether partitions can be iterated multiple
 * times; whether partitions can be empty; whether there is a partition on each machine on distributed platforms;
 * or whether partitions have a certain sorting order.</p>

- **Extends**: UnaryToUnaryOperator
- **Implements**: org

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## MaterializedGroupByOperator

**Description**: * This operator collocates the data units in a data set w.r.t. a key function.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## ObjectFileSink

**Description**: * This {@link UnarySink} writes all incoming data quanta to a Object file.
 *
 * @param <T> type of the object to store

- **Extends**: UnarySink

---

## ObjectFileSource

**Description**: * This source reads a text file and outputs the lines as data units.

- **Extends**: UnarySource
- **Implements**: org

### Public Methods:

- **getInputUrl()**: Returns `String`
- **getTypeClass()**: Returns `Class<T>`
- **estimate()**: Returns `CardinalityEstimate`

### Private Methods:

- **estimateBytesPerLine()**: Returns `OptionalDouble`

---

## PageRankOperator

**Description**: * {@link Operator} for the PageRank algorithm. It takes as input a list of directed edges, whereby each edge
 * is represented as {@code (source vertex ID, target vertex ID)} tuple. Its output are the page ranks, codified
 * as {@code (vertex ID, page rank)} tuples.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **getNumIterations()**: Returns `int`
- **getDampingFactor()**: Returns `float`
- **getGraphDensity()**: Returns `ProbabilisticDoubleInterval`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## ReduceByOperator

**Description**: * This operator groups the elements of a data set and aggregates the groups.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## ReduceOperator

**Description**: * This operator is context dependent: after a {@link GroupByOperator}, it is meant to be a {@link ReduceByOperator};
 * otherwise, it is a {@link GlobalReduceOperator}.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## RepeatOperator

**Description**: * This {@link Operator} repeats a certain subplan of {@link Operator}s for a given number of times.

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator, LoopHeadOperator

### Public Methods:

- **getState()**: Returns `State`
- **setState()**: Returns `void`
- **getType()**: Returns `DataSetType<Type>`
- **initialize()**: Returns `void`
- **beginIteration()**: Returns `void`
- **endIteration()**: Returns `void`
- **connectFinalOutputTo()**: Returns `void`
- **isReading()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getNumIterations()**: Returns `int`
- **getNumExpectedIterations()**: Returns `int`

### Private Methods:

- **initializeSlots()**: Returns `void`

---

## SampleOperator

**Description**: * A random sample operator randomly selects its inputs from the input slot and pushes that element to the output slot.

- **Extends**: UnaryToUnaryOperator

### Fields:

- **sampleMethod**: `Methods` (private)

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **getDatasetSize()**: Returns `long`
- **setDatasetSize()**: Returns `void`
- **getSampleMethod()**: Returns `Methods`
- **setSampleMethod()**: Returns `void`
- **setSeedFunction()**: Returns `void`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## SortOperator

**Description**: * This operator sorts the elements in this dataset.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## TableSource

**Description**: * {@link UnarySource} that provides the tuples from a database table.

- **Extends**: UnarySource

### Public Methods:

- **getTableName()**: Returns `String`

---

## TextFileSink

**Description**: * This {@link UnarySink} writes all incoming data quanta to a text file.

- **Extends**: UnarySink

---

## TextFileSource

**Description**: * This source reads a text file and outputs the lines as data units.

- **Extends**: UnarySource
- **Implements**: org

### Public Methods:

- **getInputUrl()**: Returns `String`
- **getEncoding()**: Returns `String`
- **estimate()**: Returns `CardinalityEstimate`

### Private Methods:

- **estimateBytesPerLine()**: Returns `OptionalDouble`

---

## UnionAllOperator

**Description**: * This {@link Operator} creates the union (bag semantics) of two .

- **Extends**: BinaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## ZipWithIdOperator

**Description**: * This operators attaches a unique ID to each input data quantum.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## WayangBasic
- **Implements**: Plugin

### Public Methods:

- **setProperties()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`

---

## WayangBasicGraph
- **Implements**: Plugin

### Public Methods:

- **setProperties()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`

---

## RecordType

**Description**: * This is a specific {@link BasicDataUnitType} for {@link Record}s. In particular, it adds schema information.

- **Extends**: BasicDataUnitType

### Public Methods:

- **isSupertypeOf()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getIndex()**: Returns `int`

---

## ProjectionDescriptorTest

**Description**: * Tests for the {@link ProjectionDescriptor}.


### Fields:

- **string**: `String` (public)
- **integer**: `int` (public)

### Public Methods:

- **testPojoImplementation()**: Returns `void`
- **testRecordImplementation()**: Returns `void`

---

## ReduceByMappingTest

**Description**: * Test suite for the {@link ReduceByMapping}.


### Public Methods:

- **testMapping()**: Returns `void`

---

## MaterializedGroupByOperatorTest

**Description**: * Tests for the {@link MaterializedGroupByOperator}.


### Public Methods:

- **testConnectingToMap()**: Returns `void`

---

## TextFileSourceTest

**Description**: * Test suite for {@link TextFileSource}.


### Public Methods:

- **testCardinalityEstimation()**: Returns `void`

---

## TestSink

**Description**: * Dummy sink for testing purposes.

- **Extends**: UnarySink

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## TestSource

**Description**: * Dummy source for testing purposes.

- **Extends**: UnarySource

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **setCardinalityEstimators()**: Returns `void`

---

## RecordTypeTest

**Description**: * Tests for the {@link RecordType}.


### Public Methods:

- **testSupertype()**: Returns `void`

---

## Configuration

**Description**: * Describes both the configuration of a {@link WayangContext} and {@link Job}s.


### Fields:

- **costSquasherProvider**: `ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>>` (private)
- **platformProvider**: `ExplicitCollectionProvider<Platform>` (private)
- **mappingProvider**: `ExplicitCollectionProvider<Mapping>` (private)
- **channelConversionProvider**: `ExplicitCollectionProvider<ChannelConversion>` (private)
- **pruningStrategyClassProvider**: `CollectionProvider<Class<PlanEnumerationPruningStrategy>>` (private)
- **instrumentationStrategyProvider**: `ValueProvider<InstrumentationStrategy>` (private)

### Public Methods:

- **load()**: Returns `void`
- **load()**: Returns `void`
- **fork()**: Returns `Configuration`
- **fork()**: Returns `Configuration`
- **setCardinalityEstimatorProvider()**: Returns `void`
- **setUdfSelectivityProvider()**: Returns `void`
- **setOperatorLoadProfileEstimatorProvider()**: Returns `void`
- **setFunctionLoadProfileEstimatorProvider()**: Returns `void`
- **setLoadProfileEstimatorCache()**: Returns `void`
- **getPlatformProvider()**: Returns `ExplicitCollectionProvider<Platform>`
- **setPlatformProvider()**: Returns `void`
- **getMappingProvider()**: Returns `ExplicitCollectionProvider<Mapping>`
- **setMappingProvider()**: Returns `void`
- **getChannelConversionProvider()**: Returns `ExplicitCollectionProvider<ChannelConversion>`
- **setChannelConversionProvider()**: Returns `void`
- **getPruningStrategyClassProvider()**: Returns `CollectionProvider<Class<PlanEnumerationPruningStrategy>>`
- **setPruningStrategyClassProvider()**: Returns `void`
- **getInstrumentationStrategyProvider()**: Returns `ValueProvider<InstrumentationStrategy>`
- **setInstrumentationStrategyProvider()**: Returns `void`
- **setPlatformStartUpTimeProvider()**: Returns `void`
- **setProperties()**: Returns `void`
- **setProperty()**: Returns `void`
- **getStringProperty()**: Returns `String`
- **getOptionalStringProperty()**: Returns `Optional<String>`
- **getStringProperty()**: Returns `String`
- **setLoadProfileToTimeConverterProvider()**: Returns `void`
- **setTimeToCostConverterProvider()**: Returns `void`
- **getCostSquasherProvider()**: Returns `ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>>`
- **setCostSquasherProvider()**: Returns `void`
- **getOptionalLongProperty()**: Returns `OptionalLong`
- **getLongProperty()**: Returns `long`
- **getLongProperty()**: Returns `long`
- **getOptionalDoubleProperty()**: Returns `OptionalDouble`
- **getDoubleProperty()**: Returns `double`
- **getDoubleProperty()**: Returns `double`
- **getOptionalBooleanProperty()**: Returns `Optional<Boolean>`
- **getBooleanProperty()**: Returns `boolean`
- **getBooleanProperty()**: Returns `boolean`
- **getParent()**: Returns `Configuration`
- **toString()**: Returns `String`
- **getName()**: Returns `String`

### Private Methods:

- **handleConfigurationFileEntry()**: Returns `void`

---

## Job

**Description**: * Describes a job that is to be executed using Wayang.

- **Extends**: OneTimeExecutable

### Fields:

- **optimizationContext**: `DefaultOptimizationContext` (private)
- **crossPlatformExecutor**: `CrossPlatformExecutor` (private)
- **cardinalityEstimatorManager**: `CardinalityEstimatorManager` (private)
- **monitor**: `Monitor` (private)
- **planImplementation**: `PlanImplementation` (private)

### Public Methods:

- **addUdfJar()**: Returns `void`
- **execute()**: Returns `void`
- **buildInitialExecutionPlan()**: Returns `ExecutionPlan`
- **reportProgress()**: Returns `void`
- **isRequestBreakpointFor()**: Returns `boolean`
- **getConfiguration()**: Returns `Configuration`
- **getUdfJarPaths()**: Returns `Set<String>`
- **getCrossPlatformExecutor()**: Returns `CrossPlatformExecutor`
- **getOptimizationContext()**: Returns `DefaultOptimizationContext`
- **getName()**: Returns `String`
- **toString()**: Returns `String`
- **getExperiment()**: Returns `Experiment`
- **getWayangPlan()**: Returns `WayangPlan`
- **getStopWatch()**: Returns `StopWatch`

### Private Methods:

- **prepareWayangPlan()**: Returns `void`
- **gatherTransformations()**: Returns `Collection<PlanTransformation>`
- **estimateKeyFigures()**: Returns `void`
- **createInitialExecutionPlan()**: Returns `ExecutionPlan`
- **pickBestExecutionPlan()**: Returns `PlanImplementation`
- **reestimateCardinalities()**: Returns `boolean`
- **createPlanEnumerator()**: Returns `PlanEnumerator`
- **createPlanEnumerator()**: Returns `PlanEnumerator`
- **execute()**: Returns `boolean`
- **setUpBreakpoint()**: Returns `void`
- **logStages()**: Returns `void`
- **postProcess()**: Returns `boolean`
- **updateExecutionPlan()**: Returns `void`
- **releaseResources()**: Returns `void`
- **logExecution()**: Returns `void`

---

## WayangContext

**Description**: * This is the entry point for users to work with Wayang.


### Fields:

- **cardinalityRepository**: `CardinalityRepository` (private)

### Public Methods:

- **with()**: Returns `WayangContext`
- **withPlugin()**: Returns `WayangContext`
- **register()**: Returns `void`
- **execute()**: Returns `void`
- **execute()**: Returns `void`
- **execute()**: Returns `void`
- **execute()**: Returns `void`
- **buildInitialExecutionPlan()**: Returns `ExecutionPlan`
- **createJob()**: Returns `Job`
- **createJob()**: Returns `Job`
- **createJob()**: Returns `Job`
- **getConfiguration()**: Returns `Configuration`
- **getCardinalityRepository()**: Returns `CardinalityRepository`

---

## CollectionProvider
- **Implements**: Iterable

### Public Methods:

- **setParent()**: Returns `void`
- **provideAll()**: Returns `Collection<Value>`
- **iterator()**: Returns `Iterator<Value>`

---

## ConstantValueProvider

**Description**: * Used by {@link Configuration}s to provide some value.

- **Extends**: ValueProvider

### Fields:

- **value**: `Value` (private)

### Public Methods:

- **setValue()**: Returns `void`

---

## ExplicitCollectionProvider

**Description**: * {@link CollectionProvider} implementation based on a blacklist and a whitelist.

- **Extends**: CollectionProvider

### Public Methods:

- **addToWhitelist()**: Returns `boolean`
- **addAllToWhitelist()**: Returns `void`
- **addToBlacklist()**: Returns `boolean`
- **addAllToBlacklist()**: Returns `void`
- **provideAll()**: Returns `Collection<Value>`

---

## FunctionalCollectionProvider

**Description**: * {@link CollectionProvider} implementation based on a blacklist and a whitelist.

- **Extends**: CollectionProvider

### Public Methods:

- **provideAll()**: Returns `Collection<Value>`

---

## FunctionalKeyValueProvider

**Description**: * Implementation of {@link KeyValueProvider} that uses a {@link Function} to provide a value.

- **Extends**: KeyValueProvider

---

## FunctionalValueProvider

**Description**: * Used by {@link Configuration}s to provide some value.

- **Extends**: ValueProvider

---

## KeyValueProvider
- **Extends**: WayangException

### Fields:

- **warningSlf4jFormat**: `String` (private)

### Public Methods:

- **provideFor()**: Returns `Value`
- **optionallyProvideFor()**: Returns `Optional<Value>`
- **provideLocally()**: Returns `Value`
- **setParent()**: Returns `void`
- **set()**: Returns `void`
- **getConfiguration()**: Returns `Configuration`

---

## MapBasedKeyValueProvider

**Description**: * Implementation of {@link KeyValueProvider} that uses a {@link Map} to provide a value.

- **Extends**: KeyValueProvider

### Public Methods:

- **tryToProvide()**: Returns `Value`
- **set()**: Returns `void`

---

## ValueProvider
- **Extends**: WayangException

### Fields:

- **warningSlf4j**: `String` (private)

### Public Methods:

- **provide()**: Returns `Value`
- **optionallyProvide()**: Returns `Optional<Value>`
- **withSlf4jWarning()**: Returns `ValueProvider<Value>`
- **getConfiguration()**: Returns `Configuration`

---

## WayangException

**Description**: * Exception that declares a problem of Wayang.

- **Extends**: RuntimeException

---

## AggregationDescriptor
- **Extends**: FunctionDescriptor

### Public Methods:

- **getInputType()**: Returns `DataUnitGroupType<InputType>`
- **getOutputType()**: Returns `BasicDataUnitType<OutputType>`
- **toString()**: Returns `String`

---

## ConsumerDescriptor

**Description**: * Created by bertty on 13-07-17.

- **Extends**: FunctionDescriptor

### Fields:

- **selectivity**: `ProbabilisticDoubleInterval` (private)

### Public Methods:

- **getJavaImplementation()**: Returns `SerializableConsumer<T>`
- **unchecked()**: Returns `SerializableConsumer<Object>`
- **getInputType()**: Returns `BasicDataUnitType<T>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

---

## ExecutionContext

---

## ExtendedFunction

---

## FlatMapDescriptor

**Description**: * This descriptor pertains to functions that consume a single data unit and output a group of data units.
 *
 * @param <Input>  input type of the transformation function
 * @param <Output> output type of the transformation function

- **Extends**: FunctionDescriptor

### Fields:

- **selectivity**: `ProbabilisticDoubleInterval` (private)

### Public Methods:

- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

---

## FunctionDescriptor
- **Extends**: Function

### Fields:

- **loadProfileEstimator**: `LoadProfileEstimator` (private)

### Public Methods:

- **setLoadProfileEstimator()**: Returns `void`
- **getLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **setLoadEstimators()**: Returns `void`

---

## MapPartitionsDescriptor

**Description**: * This descriptor pertains to functions that consume and output multiple data quanta.
 *
 * @param <Input>  input type of the transformation function
 * @param <Output> output type of the transformation function

- **Extends**: FunctionDescriptor

### Fields:

- **selectivity**: `ProbabilisticDoubleInterval` (private)

### Public Methods:

- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

---

## PredicateDescriptor

**Description**: * This descriptor pertains to predicates that consume a single data unit.
 *
 * @param <Input> input type of the transformation function

- **Extends**: FunctionDescriptor

### Fields:

- **sqlImplementation**: `String` (private)
- **selectivity**: `ProbabilisticDoubleInterval` (private)

### Public Methods:

- **getJavaImplementation()**: Returns `SerializablePredicate<Input>`
- **getSqlImplementation()**: Returns `String`
- **withSqlImplementation()**: Returns `PredicateDescriptor<Input>`
- **unchecked()**: Returns `PredicateDescriptor<Object>`
- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

---

## ReduceDescriptor

**Description**: * This descriptor pertains to functions that take multiple data units and aggregate them into a single data unit
 * by means of a tree-like fold, i.e., using a commutative, associative function..

- **Extends**: FunctionDescriptor

### Public Methods:

- **getJavaImplementation()**: Returns `BinaryOperator<Type>`
- **unchecked()**: Returns `ReduceDescriptor<Object>`
- **getInputType()**: Returns `DataUnitGroupType<Type>`
- **getOutputType()**: Returns `BasicDataUnitType<Type>`
- **toString()**: Returns `String`

---

## TransformationDescriptor

**Description**: * This descriptor pertains to functions that consume a single data unit and output a single data unit.
 *
 * @param <Input>  input type of the transformation function
 * @param <Output> output type of the transformation function

- **Extends**: FunctionDescriptor

### Public Methods:

- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **toString()**: Returns `String`

---

## Mapping

---

## OperatorMatch

**Description**: * An operator match correlates an {@link OperatorPattern} to an actually matched {@link Operator}.


### Public Methods:

- **getPattern()**: Returns `OperatorPattern`
- **getOperator()**: Returns `Operator`

---

## OperatorPattern

**Description**: * An operator pattern matches to a class of operator instances.

- **Extends**: Operator

### Public Methods:

- **match()**: Returns `OperatorMatch`
- **withAdditionalTest()**: Returns `OperatorPattern<T>`
- **getName()**: Returns `String`
- **toString()**: Returns `String`

### Private Methods:

- **matchOperatorClass()**: Returns `boolean`
- **matchSlots()**: Returns `boolean`
- **matchSlot()**: Returns `boolean`
- **matchAdditionalTests()**: Returns `boolean`
- **checkSanity()**: Returns `void`

---

## PlanTransformation

**Description**: * Looks for a {@link SubplanPattern} in a {@link WayangPlan} and replaces it with an alternative {@link Operator}s.


### Public Methods:

- **thatReplaces()**: Returns `PlanTransformation`
- **transform()**: Returns `int`
- **getTargetPlatforms()**: Returns `Collection<Platform>`

### Private Methods:

- **meetsPlatformRestrictions()**: Returns `boolean`
- **introduceAlternative()**: Returns `void`
- **replace()**: Returns `void`

---

## ReplacementSubplanFactory
- **Extends**: Operator

### Public Methods:

- **createReplacementSubplan()**: Returns `Operator`

### Private Methods:

- **setNameTo()**: Returns `void`

---

## SubplanMatch

**Description**: * A subplan match correlates a {@link SubplanPattern} with its actually matched .


### Public Methods:

- **addOperatorMatch()**: Returns `void`
- **getPattern()**: Returns `SubplanPattern`
- **getInputMatch()**: Returns `OperatorMatch`
- **getOutputMatch()**: Returns `OperatorMatch`
- **getMatch()**: Returns `OperatorMatch`
- **getMaximumEpoch()**: Returns `int`
- **getTargetPlatforms()**: Returns `Optional<Set<Platform>>`

---

## SubplanPattern

**Description**: * A subplan pattern describes a class of subplans in a {@link WayangPlan}.
 * <p><i>NB: Currently, only such patterns are tested and supported that form a chain of operators, i.e., no DAGs
 * are allowed and at most one input and one output operator.</i></p>

- **Extends**: OperatorBase

### Public Methods:

- **match()**: Returns `List<SubplanMatch>`
- **getInputPattern()**: Returns `OperatorPattern`
- **getOutputPattern()**: Returns `OperatorPattern`
- **match()**: Returns `List<SubplanMatch>`

### Private Methods:

- **attemptMatchFrom()**: Returns `void`
- **match()**: Returns `void`

---

## DisabledMonitor
- **Extends**: Monitor

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

---

## FileMonitor
- **Extends**: Monitor

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

---

## HttpMonitor

**Description**: * TODO: Implement

- **Extends**: Monitor

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

---

## Monitor

---

## ZeroMQMonitor

**Description**: * TODO: Implement

- **Extends**: Monitor

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

---

## AggregateOptimizationContext

**Description**: * This {@link OptimizationContext} implementation aggregates several {@link OptimizationContext}s and exposes
 * their {@link OptimizationContext.OperatorContext} in an aggregated manner.

- **Extends**: OptimizationContext

### Public Methods:

- **addOneTimeOperator()**: Returns `OperatorContext`
- **addOneTimeLoop()**: Returns `void`
- **getOperatorContext()**: Returns `OperatorContext`
- **updateOperatorContexts()**: Returns `void`
- **getNestedLoopContext()**: Returns `LoopContext`
- **clearMarks()**: Returns `void`
- **isTimeEstimatesComplete()**: Returns `boolean`
- **mergeToBase()**: Returns `void`
- **getDefaultOptimizationContexts()**: Returns `List<DefaultOptimizationContext>`

### Private Methods:

- **aggregateOperatorContext()**: Returns `OperatorContext`
- **updateOperatorContext()**: Returns `void`

---

## DefaultOptimizationContext

**Description**: * This implementation of {@link OptimizationContext} represents a direct mapping from {@link OptimizationContext.OperatorContext}
 * to executions of the respective {@link Operator}s.

- **Extends**: OptimizationContext

### Public Methods:

- **addOneTimeOperator()**: Returns `OperatorContext`
- **addOneTimeLoop()**: Returns `void`
- **getOperatorContext()**: Returns `OperatorContext`
- **getNestedLoopContext()**: Returns `LoopContext`
- **clearMarks()**: Returns `void`
- **isTimeEstimatesComplete()**: Returns `boolean`
- **getBase()**: Returns `DefaultOptimizationContext`
- **mergeToBase()**: Returns `void`
- **getDefaultOptimizationContexts()**: Returns `List<DefaultOptimizationContext>`
- **copy()**: Returns `DefaultOptimizationContext`

---

## OptimizationContext

**Description**: * Manages contextual information required during the optimization of a {@link WayangPlan}.
 * <p>A single {@link Operator} can have multiple contexts in a {@link WayangPlan} - namely if it appears in a loop.
 * We manage these contexts in a hierarchical fashion.</p>
 */
public abstract class OptimizationContext {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * The {@link Job} whose {@link WayangPlan} is to be optimized.
     */
    protected final Job job;

    /**
     * The instance in that this instance is nested - or {@code null} if it is top-level.
     */
    protected final LoopContext hostLoopContext;

    /**
     * The iteration number of this instance within its {@link #hostLoopContext} (starting from {@code 0}) - or {@code -1}
     * if there is no {@link #hostLoopContext}.
     */
    private int iterationNumber;

    /**
     * Forked {@link OptimizationContext}s can have a base.
     */
    private final OptimizationContext base;

    /**
     * {@link ChannelConversionGraph} used for the optimization.
     */
    private final ChannelConversionGraph channelConversionGraph;

    /**
     * {@link PlanEnumerationPruningStrategy}s to be used during optimization (in the given order).
     */
    private final List<PlanEnumerationPruningStrategy> pruningStrategies;

    /**
     * Create a new, plain instance.
     */
    public OptimizationContext(Job job) {
        this(
                job,
                null,
                null,
                -1,
                new ChannelConversionGraph(job.getConfiguration()),
                initializePruningStrategies(job.getConfiguration())
        );
    }

    /**
     * Creates a new instance. Useful for testing.
     *
     * @param operator the single {@link Operator} of this instance
     */
    public OptimizationContext(Job job, Operator operator) {
        this(
                job,
                null,
                null,
                -1,
                new ChannelConversionGraph(job.getConfiguration()),
                initializePruningStrategies(job.getConfiguration())
        );
        this.addOneTimeOperator(operator);
    }

    /**
     * Base constructor.
     */
    protected OptimizationContext(Job job,
                                  OptimizationContext base,
                                  LoopContext hostLoopContext,
                                  int iterationNumber,
                                  ChannelConversionGraph channelConversionGraph,
                                  List<PlanEnumerationPruningStrategy> pruningStrategies) {
        this.job = job;
        this.base = base;
        this.hostLoopContext = hostLoopContext;
        this.iterationNumber = iterationNumber;
        this.channelConversionGraph = channelConversionGraph;
        this.pruningStrategies = pruningStrategies;
    }

    /**
     * Initializes the {@link PlanEnumerationPruningStrategy}s from the {@link Configuration}.
     *
     * @param configuration defines the {@link PlanEnumerationPruningStrategy}s
     * @return a {@link List} of configured {@link PlanEnumerationPruningStrategy}s
     */
    private static List<PlanEnumerationPruningStrategy> initializePruningStrategies(Configuration configuration) {
        return configuration.getPruningStrategyClassProvider().provideAll().stream()
                .map(strategyClass -> OptimizationUtils.createPruningStrategy(strategyClass, configuration))
                .collect(Collectors.toList());
    }

    /**
     * Add {@link OperatorContext}s for the {@code operator} that is executed once within this instance. Also
     * add its encased {@link Operator}s.
     * Potentially invoke {@link #addOneTimeLoop(OperatorContext)} as well.
     */
    public abstract OperatorContext addOneTimeOperator(Operator operator);

    /**
     * Add {@link OperatorContext}s for all the contained {@link Operator}s of the {@code container}.
     */
    public void addOneTimeOperators(OperatorContainer container) {
        final CompositeOperator compositeOperator = container.toOperator();
        final Stream<Operator> innerOutputOperatorStream = compositeOperator.isSink() ?
                Stream.of(container.getSink()) :
                Arrays.stream(compositeOperator.getAllOutputs())
                        .map(container::traceOutput)
                        .filter(Objects::nonNull)
                        .map(Slot::getOwner);
        PlanTraversal.upstream()
                .withCallback(this::addOneTimeOperator)
                .traverse(innerOutputOperatorStream);
    }

    /**
     * Add {@link OptimizationContext}s for the {@code loop} that is executed once within this instance.
     */
    public abstract void addOneTimeLoop(OperatorContext operatorContext);

    /**
     * Return the {@link OperatorContext} of the {@code operator}.
     *
     * @param operator a one-time {@link Operator} (i.e., not in a nested loop)
     * @return the {@link OperatorContext} for the {@link Operator} or {@code null} if none
     */
    public abstract OperatorContext getOperatorContext(Operator operator);

    /**
     * Retrieve the {@link LoopContext} for the {@code loopSubplan}.
     */
    public abstract LoopContext getNestedLoopContext(LoopSubplan loopSubplan);

    /**
     * @return if this instance describes an iteration within a {@link LoopSubplan}, return the number of that iteration
     * (starting at {@code 0}); otherwise {@code -1}
     */
    public int getIterationNumber() {
        return this.iterationNumber;
    }

    /**
     * @return whether this instance is the first iteration within a {@link LoopContext}; this instance must be embedded
     * in a {@link LoopContext}
     */
    public boolean isInitialIteration() {
        assert this.hostLoopContext != null : "Not within a LoopContext.";
        return this.iterationNumber == 0;
    }


    /**
     * @return whether this instance is the final iteration within a {@link LoopContext}; this instance must be embedded
     * in a {@link LoopContext}
     */
    public boolean isFinalIteration() {
        assert this.hostLoopContext != null;
        return this.iterationNumber == this.hostLoopContext.getIterationContexts().size() - 1;
    }

    /**
     * Retrieve the {@link LoopContext} in which this instance resides.
     *
     * @return the {@link LoopContext} or {@code null} if none
     */
    public LoopContext getLoopContext() {
        return this.hostLoopContext;
    }

    /**
     * @return if this instance describes an iteration within a {@link LoopSubplan}, return the instance in which
     * this instance is nested
     */
    public OptimizationContext getParent() {
        return this.hostLoopContext == null ? null : this.hostLoopContext.getOptimizationContext();
    }

    public OptimizationContext getNextIterationContext() {
        assert this.hostLoopContext != null : String.format("%s is the last iteration.", this);
        assert !this.isFinalIteration();
        return this.hostLoopContext.getIterationContexts().get(this.iterationNumber + 1);
    }

    /**
     * Calls {@link OperatorContext#clearMarks()} for all nested {@link OperatorContext}s.
     */
    public abstract void clearMarks();

    public Configuration getConfiguration() {
        return this.job.getConfiguration();
    }

    /**
     * @return the {@link OperatorContext}s of this instance (exclusive of any base instance)
     */
    public abstract Map<Operator, OperatorContext> getLocalOperatorContexts();

    /**
     * @return whether there is a {@link TimeEstimate} for each {@link ExecutionOperator}
     */
    public abstract boolean isTimeEstimatesComplete();

    public ChannelConversionGraph getChannelConversionGraph() {
        return this.channelConversionGraph;
    }

    public OptimizationContext getBase() {
        return this.base;
    }

    public abstract void mergeToBase();

    public List<PlanEnumerationPruningStrategy> getPruningStrategies() {
        return this.pruningStrategies;
    }

    /**
     * Get the top-level parent containing this instance.
     *
     * @return the top-level parent, which can also be this instance
     */
    public OptimizationContext getRootParent() {
        OptimizationContext optimizationContext = this;
        while (true) {
            final OptimizationContext parent = optimizationContext.getParent();
            if (parent == null) return optimizationContext;
            optimizationContext = parent;
        }
    }

    /**
     * Get the {@link DefaultOptimizationContext}s represented by this instance.
     *
     * @return a {@link Collection} of said {@link DefaultOptimizationContext}s
     */
    public abstract List<DefaultOptimizationContext> getDefaultOptimizationContexts();

    /**
     * Provide the {@link Job} whose optimization is supported by this instance.
     *
     * @return the {@link Job}
     */
    public Job getJob() {
        return this.job;
    }

    /**
     * Stores a value into the {@link Job}-global cache.
     *
     * @param key   identifies the value
     * @param value the value
     * @return the value previously associated with the key or else {@code null}
     */
    public Object putIntoJobCache(String key, Object value) {
        return this.getJob().getCache().put(key, value);
    }

    /**
     * Queries the {@link Job}-global cache.
     *
     * @param key that is associated with the value to be retrieved
     * @return the value associated with the key or else {@code null}
     */
    public Object queryJobCache(String key) {
        return this.getJob().getCache().get(key);
    }

    /**
     * Queries the {@link Job}-global cache.
     *
     * @param key         that is associated with the value to be retrieved
     * @param resultClass the expected {@link Class} of the retrieved value
     * @return the value associated with the key or else {@code null}
     */
    public <T> T queryJobCache(String key, Class<T> resultClass) {
        final Object value = this.queryJobCache(key);
        try {
            return resultClass.cast(value);
        } catch (ClassCastException e) {
            throw new WayangException("Job-cache value cannot be casted as requested.", e);
        }
    }

    /**
     * Represents a single optimization context of an {@link Operator}. This can be thought of as a single, virtual
     * execution of the {@link Operator}.

- **Implements**: EstimationContext

### Fields:

- **iterationNumber**: `int` (private)
- **loadProfile**: `LoadProfile` (private)
- **costEstimate**: `ProbabilisticDoubleInterval` (private)
- **squashedCostEstimate**: `double` (private)
- **lineage**: `ExecutionLineageNode` (private)
- **aggregateOptimizationContext**: `AggregateOptimizationContext` (private)

### Public Methods:

- **addOneTimeOperators()**: Returns `void`
- **getIterationNumber()**: Returns `int`
- **isInitialIteration()**: Returns `boolean`
- **isFinalIteration()**: Returns `boolean`
- **getLoopContext()**: Returns `LoopContext`
- **getParent()**: Returns `OptimizationContext`
- **getNextIterationContext()**: Returns `OptimizationContext`
- **getConfiguration()**: Returns `Configuration`
- **getChannelConversionGraph()**: Returns `ChannelConversionGraph`
- **getBase()**: Returns `OptimizationContext`
- **getPruningStrategies()**: Returns `List<PlanEnumerationPruningStrategy>`
- **getRootParent()**: Returns `OptimizationContext`
- **getJob()**: Returns `Job`
- **putIntoJobCache()**: Returns `Object`
- **queryJobCache()**: Returns `Object`
- **getOperator()**: Returns `Operator`
- **getOutputCardinality()**: Returns `CardinalityEstimate`
- **getInputCardinality()**: Returns `CardinalityEstimate`
- **isInputMarked()**: Returns `boolean`
- **isOutputMarked()**: Returns `boolean`
- **clearMarks()**: Returns `void`
- **setInputCardinality()**: Returns `void`
- **setOutputCardinality()**: Returns `void`
- **pushCardinalitiesForward()**: Returns `void`
- **pushCardinalityForward()**: Returns `void`
- **getDoubleProperty()**: Returns `double`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getOptimizationContext()**: Returns `OptimizationContext`
- **updateCostEstimate()**: Returns `void`
- **getLoadProfileEstimator()**: Returns `LoadProfileEstimator`
- **increaseBy()**: Returns `void`
- **setNumExecutions()**: Returns `void`
- **getNumExecutions()**: Returns `int`
- **getLoadProfile()**: Returns `LoadProfile`
- **getTimeEstimate()**: Returns `TimeEstimate`
- **getCostEstimate()**: Returns `ProbabilisticDoubleInterval`
- **getSquashedCostEstimate()**: Returns `double`
- **toString()**: Returns `String`
- **resetEstimates()**: Returns `void`
- **getLoopSubplanContext()**: Returns `OperatorContext`
- **getIterationContexts()**: Returns `List<OptimizationContext>`
- **getIterationContext()**: Returns `OptimizationContext`
- **getOptimizationContext()**: Returns `OptimizationContext`
- **getInitialIterationContext()**: Returns `OptimizationContext`
- **getFinalIterationContext()**: Returns `OptimizationContext`
- **appendIterationContext()**: Returns `OptimizationContext`
- **getLoop()**: Returns `LoopSubplan`
- **getAggregateContext()**: Returns `AggregateOptimizationContext`

### Private Methods:

- **updateCostEstimate()**: Returns `void`
- **addTo()**: Returns `void`
- **addTo()**: Returns `void`

---

## OptimizationUtils

**Description**: * Utility methods for the optimization process.

- **Extends**: PlanEnumerationPruningStrategy

---

## ProbabilisticDoubleInterval

**Description**: *
 * An value representation that is capable of expressing uncertainty.
 * It addresses uncertainty by expressing estimates as intervals and assigning a probability of correctness (in [0, 1]).


### Public Methods:

- **getLowerEstimate()**: Returns `double`
- **getUpperEstimate()**: Returns `double`
- **getAverageEstimate()**: Returns `double`
- **getGeometricMeanEstimate()**: Returns `long`
- **getCorrectnessProbability()**: Returns `double`
- **isExactly()**: Returns `boolean`
- **plus()**: Returns `ProbabilisticDoubleInterval`
- **equals()**: Returns `boolean`
- **equalsWithinDelta()**: Returns `boolean`
- **isOverride()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

---

## ProbabilisticIntervalEstimate

**Description**: *
 * An estimate that is capable of expressing uncertainty.
 * The estimate addresses uncertainty in the estimation process by
 * expressing estimates as intervals
 * and assigning a probability of correctness (in [0, 1]).
 **

- **Extends**: ProbabilisticIntervalEstimate

### Public Methods:

- **getLowerEstimate()**: Returns `long`
- **getUpperEstimate()**: Returns `long`
- **getAverageEstimate()**: Returns `long`
- **getGeometricMeanEstimate()**: Returns `long`
- **getCorrectnessProbability()**: Returns `double`
- **isExactly()**: Returns `boolean`
- **isExact()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **equalsWithinDelta()**: Returns `boolean`
- **isOverride()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

---

## SanityChecker

**Description**: * This class checks a {@link WayangPlan} for several sanity criteria:
 * <ol>
 * <li>{@link Subplan}s must only be used as top-level {@link Operator} of {@link OperatorAlternative.Alternative}</li>
 * <li>{@link Subplan}s must contain more than one {@link Operator}</li>
 * </ol>


### Public Methods:

- **checkAllCriteria()**: Returns `boolean`
- **checkProperSubplans()**: Returns `boolean`
- **checkFlatAlternatives()**: Returns `boolean`

### Private Methods:

- **checkSubplanNotASingleton()**: Returns `void`
- **traverse()**: Returns `PlanTraversal`

---

## AbstractAlternativeCardinalityPusher
- **Extends**: CardinalityPusher

---

## AggregatingCardinalityEstimator

**Description**: * {@link CardinalityEstimator} implementation that can have multiple ways of calculating a {@link CardinalityEstimate}.

- **Implements**: CardinalityEstimator

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## CardinalityEstimate

**Description**: * An estimate of cardinality within a {@link WayangPlan} expressed as a {@link ProbabilisticIntervalEstimate}.

- **Extends**: ProbabilisticIntervalEstimate
- **Implements**: JsonSerializable

### Public Methods:

- **plus()**: Returns `CardinalityEstimate`
- **divideBy()**: Returns `CardinalityEstimate`
- **toJson()**: Returns `WayangJsonObj`
- **toJson()**: Returns `WayangJsonObj`
- **toString()**: Returns `String`

---

## CardinalityEstimationTraversal

**Description**: * {@link CardinalityEstimator} that subsumes a DAG of operators, each one providing a local {@link CardinalityEstimator}.

- **Extends**: Activator

### Fields:

- **result**: `CardinalityEstimationTraversal` (private)

### Public Methods:

- **traverse()**: Returns `boolean`
- **toString()**: Returns `String`
- **getTargetActivator()**: Returns `Activator`
- **fire()**: Returns `void`
- **doExecute()**: Returns `void`

### Private Methods:

- **initializeActivatorQueue()**: Returns `Queue<Activator>`
- **reset()**: Returns `void`
- **resetAll()**: Returns `void`
- **resetDownstream()**: Returns `void`
- **processDependentActivations()**: Returns `void`
- **addAndRegisterActivator()**: Returns `void`

---

## CardinalityEstimator

---

## CardinalityEstimatorManager

**Description**: * Handles the {@link CardinalityEstimate}s of a {@link WayangPlan}.


### Fields:

- **planTraversal**: `CardinalityEstimationTraversal` (private)

### Public Methods:

- **pushCardinalities()**: Returns `boolean`
- **pushCardinalities()**: Returns `boolean`
- **updateConversionOperatorCardinalities()**: Returns `void`
- **getPlanTraversal()**: Returns `CardinalityEstimationTraversal`
- **pushCardinalityUpdates()**: Returns `boolean`

### Private Methods:

- **injectMeasuredCardinalities()**: Returns `boolean`
- **injectMeasuredCardinality()**: Returns `void`
- **injectMeasuredCardinality()**: Returns `void`

---

## CardinalityPusher

### Public Methods:

- **push()**: Returns `boolean`

---

## DefaultCardinalityEstimator

**Description**: * Default implementation of the {@link CardinalityEstimator}. Generalizes a single-point estimation function.

- **Implements**: CardinalityEstimator

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## DefaultCardinalityPusher

**Description**: * Default {@link CardinalityPusher} implementation. Bundles all {@link CardinalityEstimator}s of an {@link Operator}.

- **Extends**: CardinalityPusher

---

## FallbackCardinalityEstimator

**Description**: * Assumes with a confidence of 50% that the output cardinality will be somewhere between 1 and the product of
 * all 10*input estimates.

- **Implements**: CardinalityEstimator

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## FixedSizeCardinalityEstimator

**Description**: * {@link CardinalityEstimator} implementation for {@link Operator}s with a fix-sized output.

- **Implements**: CardinalityEstimator

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## LoopHeadAlternativeCardinalityPusher

**Description**: * {@link CardinalityPusher} implementation for {@link LoopHeadAlternative}s.

- **Extends**: AbstractAlternativeCardinalityPusher

### Public Methods:

- **pushThroughAlternatives()**: Returns `void`

---

## LoopSubplanCardinalityPusher

**Description**: * {@link CardinalityPusher} implementation for {@link LoopSubplan}s.

- **Extends**: CardinalityPusher

---

## OperatorAlternativeCardinalityPusher

**Description**: * {@link CardinalityPusher} implementation for {@link OperatorAlternative}s.

- **Extends**: AbstractAlternativeCardinalityPusher

### Public Methods:

- **pushThroughAlternatives()**: Returns `void`

### Private Methods:

- **pushThroughPath()**: Returns `void`

---

## SubplanCardinalityPusher

**Description**: * {@link CardinalityPusher} implementation for {@link Subplan}s (but not for {@link LoopSubplan}s!)

- **Extends**: CardinalityPusher

---

## SwitchForwardCardinalityEstimator

**Description**: * Forwards the {@link CardinalityEstimate} of any given {@link InputSlot} that is not {@code null}. Asserts that
 * all other {@link CardinalityEstimate}s are indeed {@code null}.

- **Implements**: CardinalityEstimator

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

---

## ChannelConversion

### Public Methods:

- **convert()**: Returns `Channel`
- **getSourceChannelDescriptor()**: Returns `ChannelDescriptor`
- **getTargetChannelDescriptor()**: Returns `ChannelDescriptor`
- **toString()**: Returns `String`

---

## ChannelConversionGraph

**Description**: * This graph contains a set of {@link ChannelConversion}s.

- **Extends**: OneTimeExecutable
- **Implements**: TreeSelectionStrategy

### Fields:

- **root**: `TreeVertex` (private)

### Public Methods:

- **add()**: Returns `void`
- **findMinimumCostJunction()**: Returns `Junction`
- **findMinimumCostJunction()**: Returns `Junction`
- **select()**: Returns `Tree`
- **select()**: Returns `Tree`
- **getJunction()**: Returns `Junction`
- **toString()**: Returns `String`
- **toString()**: Returns `String`
- **toString()**: Returns `String`

### Private Methods:

- **getOrCreateChannelConversions()**: Returns `List<ChannelConversion>`
- **selectPreferredTree()**: Returns `Tree`
- **mergeTrees()**: Returns `Tree`
- **collectExistingChannels()**: Returns `void`
- **resolveSupportedChannels()**: Returns `Set<ChannelDescriptor>`
- **kernelizeChannelRequests()**: Returns `void`
- **searchTree()**: Returns `Tree`
- **getSuccessorChannelDescriptors()**: Returns `Set<ChannelDescriptor>`
- **getCostEstimate()**: Returns `double`
- **isFiltered()**: Returns `boolean`
- **createJunction()**: Returns `void`
- **createJunctionAux()**: Returns `void`
- **forkLocalOptimizationContext()**: Returns `List<OptimizationContext>`
- **linkTo()**: Returns `TreeEdge`
- **copyEdgesFrom()**: Returns `void`
- **getChildChannelConversions()**: Returns `Set<ChannelConversion>`

---

## DefaultChannelConversion

**Description**: * Default implementation of the {@link ChannelConversion}. Can be used without further subclassing.

- **Extends**: ChannelConversion

### Public Methods:

- **convert()**: Returns `Channel`
- **update()**: Returns `void`
- **estimateConversionCost()**: Returns `ProbabilisticDoubleInterval`
- **isFiltered()**: Returns `boolean`
- **toString()**: Returns `String`

### Private Methods:

- **setCardinalityAndTimeEstimates()**: Returns `void`
- **determineCardinality()**: Returns `CardinalityEstimate`
- **setCardinalityAndTimeEstimate()**: Returns `void`
- **setCardinality()**: Returns `void`

---

## ConstantLoadProfileEstimator

**Description**: * {@link LoadProfileEstimator} that estimates a predefined {@link LoadProfile}.

- **Implements**: LoadProfileEstimator

### Public Methods:

- **nest()**: Returns `void`
- **estimate()**: Returns `LoadProfile`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **copy()**: Returns `LoadProfileEstimator`

---

## DefaultLoadEstimator

**Description**: * Implementation of {@link LoadEstimator} that uses a single-point cost function.

- **Extends**: LoadEstimator

### Public Methods:

- **calculate()**: Returns `LoadEstimate`

---

## EstimationContext
- **Extends**: EstimationContext

### Public Methods:

- **getDoubleProperty()**: Returns `double`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getNumExecutions()**: Returns `int`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `EstimationContext`

---

## IntervalLoadEstimator

**Description**: * Implementation of {@link LoadEstimator} that uses a interval-based cost function.

- **Extends**: LoadEstimator

### Public Methods:

- **calculate()**: Returns `LoadEstimate`

---

## LoadEstimate

**Description**: * An estimate of costs of some executable code expressed as a {@link ProbabilisticIntervalEstimate}.

- **Extends**: ProbabilisticIntervalEstimate
- **Implements**: JsonSerializable

### Public Methods:

- **times()**: Returns `LoadEstimate`
- **plus()**: Returns `LoadEstimate`
- **toJson()**: Returns `WayangJsonObj`

---

## LoadEstimator

### Private Methods:

- **calculateJointProbability()**: Returns `double`

---

## LoadProfile

**Description**: * Reflects the (estimated) required resources of an {@link Operator} or {@link FunctionDescriptor}.

- **Implements**: JsonSerializable

### Fields:

- **overheadMillis**: `long` (private)

### Public Methods:

- **getCpuUsage()**: Returns `LoadEstimate`
- **getRamUsage()**: Returns `LoadEstimate`
- **getNetworkUsage()**: Returns `LoadEstimate`
- **getDiskUsage()**: Returns `LoadEstimate`
- **getSubprofiles()**: Returns `Collection<LoadProfile>`
- **nest()**: Returns `void`
- **getOverheadMillis()**: Returns `long`
- **setOverheadMillis()**: Returns `void`
- **getResourceUtilization()**: Returns `double`
- **setResourceUtilization()**: Returns `void`
- **timesSequential()**: Returns `LoadProfile`
- **plus()**: Returns `LoadProfile`
- **toJson()**: Returns `WayangJsonObj`

---

## LoadProfileEstimator

---

## LoadProfileEstimators

**Description**: * Utilities to deal with {@link LoadProfileEstimator}s.

- **Extends**: ExecutionOperator

### Public Methods:

- **getVariable()**: Returns `double`

---

## LoadProfileToTimeConverter

### Public Methods:

- **convert()**: Returns `TimeEstimate`

### Private Methods:

- **sumWithSubprofiles()**: Returns `TimeEstimate`

---

## LoadToTimeConverter

### Public Methods:

- **convert()**: Returns `TimeEstimate`
- **toString()**: Returns `String`

---

## NestableLoadProfileEstimator

**Description**: * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.

- **Implements**: LoadProfileEstimator

### Public Methods:

- **nest()**: Returns `void`
- **estimate()**: Returns `LoadProfile`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **copy()**: Returns `LoadProfileEstimator`
- **toString()**: Returns `String`

### Private Methods:

- **performLocalEstimation()**: Returns `LoadProfile`
- **estimateResourceUtilization()**: Returns `double`
- **getOverheadMillis()**: Returns `long`

---

## SimpleEstimationContext

**Description**: * This {@link EstimationContext} implementation just stores all required variables without any further logic.

- **Extends**: SimpleEstimationContext
- **Implements**: EstimationContext

### Public Methods:

- **getDoubleProperty()**: Returns `double`
- **getNumExecutions()**: Returns `int`
- **getPropertyKeys()**: Returns `Collection<String>`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `SimpleEstimationContext`
- **deserialize()**: Returns `SimpleEstimationContext`

---

## TimeEstimate

**Description**: * An estimate of time (in <b>milliseconds</b>) expressed as a {@link ProbabilisticIntervalEstimate}.

- **Extends**: ProbabilisticIntervalEstimate

### Public Methods:

- **plus()**: Returns `TimeEstimate`
- **plus()**: Returns `TimeEstimate`
- **times()**: Returns `TimeEstimate`
- **toString()**: Returns `String`
- **toIntervalString()**: Returns `String`
- **toGMeanString()**: Returns `String`

---

## TimeToCostConverter

**Description**: * This (linear) converter turns {@link TimeEstimate}s into cost estimates.


### Public Methods:

- **convert()**: Returns `ProbabilisticDoubleInterval`
- **convertWithoutFixCosts()**: Returns `ProbabilisticDoubleInterval`
- **getFixCosts()**: Returns `double`
- **getCostsPerMillisecond()**: Returns `double`

---

## ExecutionTaskFlow

**Description**: * Graph of {@link ExecutionTask}s and {@link Channel}s. Does not define {@link ExecutionStage}s and
 * {@link PlatformExecution}s - in contrast to a final {@link ExecutionPlan}.


### Public Methods:

- **collectAllTasks()**: Returns `Set<ExecutionTask>`
- **isComplete()**: Returns `boolean`
- **getSinkTasks()**: Returns `Collection<ExecutionTask>`

### Private Methods:

- **collectAllTasksAux()**: Returns `void`
- **collectAllTasksAux()**: Returns `void`

---

## ExecutionTaskFlowCompiler

**Description**: * Creates an {@link ExecutionTaskFlow} from a {@link PlanImplementation}.

- **Extends**: AbstractTopologicalTraversal

### Fields:

- **executionTask**: `ExecutionTask` (private)

### Public Methods:

- **getTerminalTasks()**: Returns `Collection<ExecutionTask>`
- **getInputChannels()**: Returns `Set<Channel>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Private Methods:

- **getOrCreateExecutionTask()**: Returns `ExecutionTask`
- **connectToSuccessorTasks()**: Returns `void`
- **getJunction()**: Returns `Junction`
- **createActivation()**: Returns `void`

---

## LatentOperatorPruningStrategy

**Description**: * This {@link PlanEnumerationPruningStrategy} follows the idea that we can prune a
 * {@link PlanImplementation}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.

- **Implements**: PlanEnumerationPruningStrategy

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`

### Private Methods:

- **selectBestPlanNary()**: Returns `PlanImplementation`
- **selectBestPlanBinary()**: Returns `PlanImplementation`

---

## LoopEnumerator

**Description**: * Enumerator for {@link LoopSubplan}s.

- **Extends**: OneTimeExecutable

### Fields:

- **loopEnumeration**: `PlanEnumeration` (private)

### Public Methods:

- **enumerate()**: Returns `PlanEnumeration`

### Private Methods:

- **addFeedbackConnections()**: Returns `void`
- **addFeedbackConnection()**: Returns `boolean`

---

## LoopImplementation

**Description**: * Describes the enumeration of a {@link LoopSubplan}.

- **Implements**: the given

### Fields:

- **interBodyJunction**: `Junction` (private)
- **forwardJunction**: `Junction` (private)
- **enterJunction**: `Junction` (private)
- **exitJunction**: `Junction` (private)

### Public Methods:

- **addIterationEnumeration()**: Returns `IterationImplementation`
- **getTimeEstimate()**: Returns `TimeEstimate`
- **getCostEstimate()**: Returns `ProbabilisticDoubleInterval`
- **getSquashedCostEstimate()**: Returns `double`
- **getIterationImplementations()**: Returns `List<IterationImplementation>`
- **getSingleIterationImplementation()**: Returns `IterationImplementation`
- **getNumIterations()**: Returns `int`
- **getBodyImplementation()**: Returns `PlanImplementation`
- **getInterBodyJunction()**: Returns `Junction`
- **setInterBodyJunction()**: Returns `void`
- **getForwardJunction()**: Returns `Junction`
- **setForwardJunction()**: Returns `void`
- **getEnterJunction()**: Returns `Junction`
- **setEnterJunction()**: Returns `void`
- **getExitJunction()**: Returns `Junction`
- **setExitJunction()**: Returns `void`
- **getTimeEstimate()**: Returns `TimeEstimate`
- **getCostEstimate()**: Returns `ProbabilisticDoubleInterval`
- **getSquashedCostEstimate()**: Returns `double`
- **getLoopImplementation()**: Returns `LoopImplementation`
- **getSuccessorIterationImplementation()**: Returns `IterationImplementation`
- **getJunction()**: Returns `Junction`

---

## PlanEnumeration

**Description**: * Represents a collection of {@link PlanImplementation}s that all implement the same section of a {@link WayangPlan} (which
 * is assumed to contain {@link OperatorAlternative}s in general).
 * <p>Instances can be mutated and combined in algebraic manner. In particular, instances can be unioned if they implement
 * the same part of the {@link WayangPlan}, concatenated if there are contact points, and pruned.</p>


### Public Methods:

- **concatenate()**: Returns `PlanEnumeration`
- **add()**: Returns `void`
- **unionInPlace()**: Returns `void`
- **escape()**: Returns `PlanEnumeration`
- **getPlanImplementations()**: Returns `Collection<PlanImplementation>`
- **getScope()**: Returns `Set<OperatorAlternative>`
- **toString()**: Returns `String`

### Private Methods:

- **concatenatePartialPlans()**: Returns `Collection<PlanImplementation>`
- **concatenatePartialPlansBatchwise()**: Returns `Collection<PlanImplementation>`
- **createSingletonPartialPlan()**: Returns `PlanImplementation`
- **toIOString()**: Returns `String`
- **toScopeString()**: Returns `String`

---

## PlanEnumerationPruningStrategy

---

## PlanEnumerator

**Description**: * The plan partitioner recursively dissects a {@link WayangPlan} into {@link PlanEnumeration}s and then assembles
 * them.


### Fields:

- **resultReference**: `AtomicReference<PlanEnumeration>` (private)
- **timeMeasurement**: `TimeMeasurement` (private)
- **isEnumeratingBranchesFirst**: `boolean` (private)
- **baseEnumeration**: `PlanEnumeration` (private)

### Public Methods:

- **enumerate()**: Returns `PlanEnumeration`
- **deemsComprehensive()**: Returns `boolean`
- **isTopLevel()**: Returns `boolean`
- **getConfiguration()**: Returns `Configuration`
- **getOperator()**: Returns `Operator`
- **getOptimizationContext()**: Returns `OptimizationContext`
- **toString()**: Returns `String`
- **getBaseEnumeration()**: Returns `PlanEnumeration`
- **updateBaseEnumeration()**: Returns `void`
- **getOptimizationContext()**: Returns `OptimizationContext`
- **toString()**: Returns `String`
- **setTimeMeasurement()**: Returns `void`

### Private Methods:

- **scheduleForEnumeration()**: Returns `void`
- **enumerateBranchStartingFrom()**: Returns `void`
- **collectBranchOperatorsStartingFrom()**: Returns `List<Operator>`
- **enumerateBranch()**: Returns `PlanEnumeration`
- **enumerateAlternative()**: Returns `PlanEnumeration`
- **forkFor()**: Returns `PlanEnumerator`
- **enumerateLoop()**: Returns `PlanEnumeration`
- **concatenate()**: Returns `void`
- **postProcess()**: Returns `void`
- **activateDownstream()**: Returns `int`
- **activateDownstreamEnumeration()**: Returns `boolean`
- **activateUpstream()**: Returns `void`
- **activateUpstreamConcatenation()**: Returns `void`
- **getOrCreateConcatenationActivator()**: Returns `ConcatenationActivator`
- **constructResultEnumeration()**: Returns `void`
- **prune()**: Returns `void`
- **canBeActivated()**: Returns `boolean`
- **requiresActivation()**: Returns `boolean`
- **register()**: Returns `void`
- **canBeActivated()**: Returns `boolean`
- **register()**: Returns `void`
- **updatePriority()**: Returns `void`
- **estimateNumConcatenatedPlanImplementations()**: Returns `double`
- **estimateNumConcatenatedPlanImplementations2()**: Returns `double`
- **countNumOfOpenSlots()**: Returns `double`

---

## PlanImplementation

**Description**: * Represents a partial execution plan.


### Fields:

- **planEnumeration**: `PlanEnumeration` (private)
- **platformCache**: `Set<Platform>` (private)

### Public Methods:

- **getPlanEnumeration()**: Returns `PlanEnumeration`
- **setPlanEnumeration()**: Returns `void`
- **escape()**: Returns `PlanImplementation`
- **getOperators()**: Returns `Canonicalizer<ExecutionOperator>`
- **addLoopImplementation()**: Returns `void`
- **getInterfaceOperators()**: Returns `Collection<ExecutionOperator>`
- **getStartOperators()**: Returns `List<ExecutionOperator>`
- **getTimeEstimate()**: Returns `TimeEstimate`
- **getTimeEstimate()**: Returns `TimeEstimate`
- **getCostEstimate()**: Returns `ProbabilisticDoubleInterval`
- **getSquashedCostEstimate()**: Returns `double`
- **getJunction()**: Returns `Junction`
- **putJunction()**: Returns `void`
- **getOptimizationContext()**: Returns `OptimizationContext`
- **mergeJunctionOptimizationContexts()**: Returns `void`
- **logTimeEstimates()**: Returns `void`
- **getUtilizedPlatforms()**: Returns `Set<Platform>`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Private Methods:

- **copyLoopImplementations()**: Returns `PlanImplementation`
- **isSettledAlternativesContradicting()**: Returns `boolean`
- **allOutermostOutputSlots()**: Returns `Stream<OutputSlot>`
- **allOutermostInputSlots()**: Returns `Stream<InputSlot>`
- **isStartOperator()**: Returns `boolean`

---

## RandomPruningStrategy
- **Implements**: PlanEnumerationPruningStrategy

### Fields:

- **random**: `Random` (private)
- **numRetainedPlans**: `int` (private)

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`

---

## SinglePlatformPruningStrategy
- **Implements**: PlanEnumerationPruningStrategy

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`

---

## StageAssignmentTraversal

**Description**: * Builds an {@link ExecutionPlan} from a {@link ExecutionTaskFlow}.
 * <p>Specifically, subdivides the {@link ExecutionTask}s into {@link PlatformExecution}s and {@link ExecutionStage}s,
 * thereby discarding already executed {@link ExecutionTask}s.</p>
 * <p>As of now, these are recognized as producers of
 * {@link Channel}s that are copied (see {@link Channel#isCopy()}). This is because of {@link ExecutionTaskFlowCompiler} that copies {@link Channel}s
 * to different alternative {@link ExecutionPlan}s on top of existing fixed {@link ExecutionTask}s.</p>

- **Extends**: OneTimeExecutable
- **Implements**: InterimStage

### Fields:

- **result**: `ExecutionPlan` (private)
- **isMarked**: `boolean` (private)

### Public Methods:

- **getPlatform()**: Returns `Platform`
- **addTask()**: Returns `void`
- **setOutbound()**: Returns `void`
- **getOutboundTasks()**: Returns `Set<ExecutionTask>`
- **getStartTasks()**: Returns `Collection<ExecutionTask>`
- **getTasks()**: Returns `Set<ExecutionTask>`
- **separate()**: Returns `InterimStage`
- **createSplit()**: Returns `InterimStage`
- **markDependenciesUpdated()**: Returns `void`
- **getAndResetSplitMark()**: Returns `boolean`
- **toString()**: Returns `String`
- **toExecutionStage()**: Returns `ExecutionStage`
- **getTasks()**: Returns `Set<ExecutionTask>`
- **getPlatform()**: Returns `Platform`
- **addTask()**: Returns `void`
- **setOutbound()**: Returns `void`
- **toExecutionStage()**: Returns `ExecutionStage`
- **separate()**: Returns `InterimStage`
- **getAndResetSplitMark()**: Returns `boolean`
- **mark()**: Returns `void`
- **getOutboundTasks()**: Returns `Set<ExecutionTask>`

### Private Methods:

- **buildExecutionPlan()**: Returns `ExecutionPlan`
- **discoverInitialStages()**: Returns `void`
- **checkIfShouldReusePlatformExecution()**: Returns `boolean`
- **createStageFor()**: Returns `void`
- **addStage()**: Returns `void`
- **assignTaskAndExpand()**: Returns `void`
- **assign()**: Returns `void`
- **expandDownstream()**: Returns `void`
- **expandUpstream()**: Returns `void`
- **handleTaskWithoutPlatformExecution()**: Returns `void`
- **refineStages()**: Returns `void`
- **applySplittingCriteria()**: Returns `void`
- **separateConnectedComponent()**: Returns `Set<ExecutionTask>`
- **splitStagesByPrecedence()**: Returns `void`
- **updateRequiredStages()**: Returns `void`
- **partitionStage()**: Returns `boolean`
- **splitStage()**: Returns `InterimStage`
- **assembleExecutionPlan()**: Returns `ExecutionPlan`
- **assembleExecutionPlan()**: Returns `void`
- **shouldVisitProducerOf()**: Returns `boolean`
- **checkIfStartTask()**: Returns `boolean`
- **checkIfFeedbackChannel()**: Returns `boolean`
- **checkIfTerminalTask()**: Returns `boolean`
- **checkIfFeedforwardChannel()**: Returns `boolean`

---

## TopKPruningStrategy

**Description**: * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.

- **Implements**: PlanEnumerationPruningStrategy

### Fields:

- **k**: `int` (private)

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`

### Private Methods:

- **comparePlanImplementations()**: Returns `int`

---

## EnumerationAlternative

**Description**: * An enumeration alternative is embedded within an {@link EnumerationBranch}.


---

## EnumerationBranch

**Description**: * An enumeration branch is the basic unit for enumeration, i.e., translation from a {@link WayangPlan} to an
 * {@link ExecutionPlan}.


---

## Channel
- **Extends**: OutputSlot

### Fields:

- **producer**: `ExecutionTask` (private)

### Public Methods:

- **addConsumer()**: Returns `void`
- **isReusable()**: Returns `boolean`
- **isSuitableForBreakpoint()**: Returns `boolean`
- **isExecutionBreaker()**: Returns `boolean`
- **getProducer()**: Returns `ExecutionTask`
- **getConsumers()**: Returns `List<ExecutionTask>`
- **getCardinalityEstimate()**: Returns `CardinalityEstimate`
- **isMarkedForInstrumentation()**: Returns `boolean`
- **withSiblings()**: Returns `Stream<Channel>`
- **markForInstrumentation()**: Returns `void`
- **toString()**: Returns `String`
- **addSibling()**: Returns `void`
- **removeSiblings()**: Returns `void`
- **removeSiblingsWhere()**: Returns `void`
- **retain()**: Returns `boolean`
- **getOriginal()**: Returns `Channel`
- **isCopy()**: Returns `boolean`
- **mergeIntoOriginal()**: Returns `void`
- **getDescriptor()**: Returns `ChannelDescriptor`
- **getProducerOperator()**: Returns `ExecutionOperator`
- **getSiblings()**: Returns `Set<Channel>`
- **isBetweenStages()**: Returns `boolean`

### Private Methods:

- **withSiblings()**: Returns `Stream<Channel>`
- **relateTo()**: Returns `void`
- **copyConsumersFrom()**: Returns `void`
- **adoptSiblings()**: Returns `void`

---

## ChannelInitializer

---

## ExecutionPlan

**Description**: * Represents an executable, cross-platform data flow. Consists of muliple {@link PlatformExecution}s.

- **Extends**: this

### Public Methods:

- **addStartingStage()**: Returns `void`
- **getStartingStages()**: Returns `Collection<ExecutionStage>`
- **toExtensiveString()**: Returns `String`
- **toExtensiveString()**: Returns `String`
- **toJsonList()**: Returns `List<Map>`
- **retain()**: Returns `Set<Channel>`
- **getStages()**: Returns `Set<ExecutionStage>`
- **collectAllTasks()**: Returns `Set<ExecutionTask>`
- **expand()**: Returns `void`
- **getOpenInputChannels()**: Returns `Collection<Channel>`
- **isSane()**: Returns `boolean`

---

## ExecutionStage

**Description**: * Resides within a {@link PlatformExecution} and represents the minimum execution unit that is controlled by Wayang.
 * <p>The purpose of stages is to allow to do only a part of work that is to be done by a single
 * {@link PlatformExecution} and invoke a further {@link PlatformExecution} to proceed working with the results
 * of this stage. Also, this allows to consume data with a {@link PlatformExecution} only when it is needed, i.e.,
 * at a deferred stage. However, the level of control that can be imposed by Wayang can vary between {@link Platform}s</p>
 * <p>Note that this class is immutable, i.e., it does not comprise any execution state.</p>


### Public Methods:

- **addSuccessor()**: Returns `void`
- **getPlatformExecution()**: Returns `PlatformExecution`
- **getPredecessors()**: Returns `Collection<ExecutionStage>`
- **getSuccessors()**: Returns `Collection<ExecutionStage>`
- **addTask()**: Returns `void`
- **isLoopHead()**: Returns `boolean`
- **getLoop()**: Returns `ExecutionStageLoop`
- **getLoopHeadTask()**: Returns `ExecutionTask`
- **isInFinishedLoop()**: Returns `boolean`
- **markAsStartTask()**: Returns `void`
- **markAsTerminalTask()**: Returns `void`
- **getStartTasks()**: Returns `Collection<ExecutionTask>`
- **isStartingStage()**: Returns `boolean`
- **toString()**: Returns `String`
- **toNameString()**: Returns `String`
- **getTerminalTasks()**: Returns `Collection<ExecutionTask>`
- **getOutboundChannels()**: Returns `Collection<Channel>`
- **getInboundChannels()**: Returns `Collection<Channel>`
- **getPlanAsString()**: Returns `String`
- **getPlanAsString()**: Returns `String`
- **getPlanAsString()**: Returns `void`
- **toJsonMap()**: Returns `Map`
- **getAllTasks()**: Returns `Set<ExecutionTask>`
- **retainSuccessors()**: Returns `void`

### Private Methods:

- **updateLoop()**: Returns `void`
- **toExtensiveStringAux()**: Returns `void`
- **toJsonMapAux()**: Returns `void`
- **prettyPrint()**: Returns `String`
- **prettyPrint()**: Returns `String`

---

## ExecutionStageLoop

**Description**: * This class models the execution equivalent of {@link LoopSubplan}s.


### Fields:

- **headStageCache**: `ExecutionStage` (private)

### Public Methods:

- **add()**: Returns `void`
- **update()**: Returns `void`
- **getLoopHead()**: Returns `ExecutionStage`
- **getLoopSubplan()**: Returns `LoopSubplan`

### Private Methods:

- **checkForLoopHead()**: Returns `boolean`
- **isLoopHead()**: Returns `boolean`

---

## ExecutionTask

**Description**: * Serves as an adapter to include {@link ExecutionOperator}s, which are usually parts of {@link WayangPlan}s, in
 * {@link ExecutionPlan}s.

- **Implements**: a feedback

### Fields:

- **stage**: `ExecutionStage` (private)

### Public Methods:

- **getOperator()**: Returns `ExecutionOperator`
- **getNumInputChannels()**: Returns `int`
- **getInputChannel()**: Returns `Channel`
- **exchangeInputChannel()**: Returns `void`
- **getNumOuputChannels()**: Returns `int`
- **getOutputChannel()**: Returns `Channel`
- **removeOutputChannel()**: Returns `int`
- **removeInputChannel()**: Returns `int`
- **initializeOutputChannel()**: Returns `Channel`
- **setOutputChannel()**: Returns `void`
- **getStage()**: Returns `ExecutionStage`
- **setStage()**: Returns `void`
- **toString()**: Returns `String`
- **isFeedbackInput()**: Returns `boolean`
- **getPlatform()**: Returns `Platform`

---

## PlatformExecution

**Description**: * Complete data flow on a single platform, that consists of multiple {@link ExecutionStage}s.


### Public Methods:

- **getStages()**: Returns `Collection<ExecutionStage>`
- **getPlatform()**: Returns `Platform`
- **createStage()**: Returns `ExecutionStage`
- **toString()**: Returns `String`
- **retain()**: Returns `void`

---

## ActualOperator
- **Extends**: Operator

---

## BinaryToUnaryOperator
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator

### Public Methods:

- **getInputType0()**: Returns `DataSetType<InputType0>`
- **getInputType1()**: Returns `DataSetType<InputType1>`
- **getOutputType()**: Returns `DataSetType<OutputType>`

---

## CompositeOperator
- **Extends**: Operator

---

## ElementaryOperator
- **Extends**: ActualOperator

---

## EstimationContextProperty

---

## ExecutionOperator
- **Extends**: ElementaryOperator

---

## InputSlot

**Description**: * An input slot declares an input of an {@link Operator}.
 *
 * @param <T> see {@link Slot}

- **Extends**: Slot

### Fields:

- **occupant**: `OutputSlot<T>` (private)

### Public Methods:

- **stealOccupant()**: Returns `void`
- **copyFor()**: Returns `InputSlot<T>`
- **copyAsNonBroadcastFor()**: Returns `InputSlot<T>`
- **getOccupant()**: Returns `OutputSlot<T>`
- **getIndex()**: Returns `int`
- **unchecked()**: Returns `InputSlot<Object>`
- **isBroadcast()**: Returns `boolean`
- **isFeedback()**: Returns `boolean`
- **notifyDetached()**: Returns `void`
- **isLoopInvariant()**: Returns `boolean`

---

## LoopHeadAlternative

**Description**: * Special {@link OperatorAlternative} for {@link LoopHeadOperator}s.

- **Extends**: OperatorAlternative
- **Implements**: LoopHeadOperator

### Public Methods:

- **addAlternative()**: Returns `Alternative`
- **getNumExpectedIterations()**: Returns `int`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **getInitializationPusher()**: Returns `CardinalityPusher`
- **getFinalizationPusher()**: Returns `CardinalityPusher`

---

## LoopHeadOperator
- **Extends**: Operator

---

## LoopIsolator

**Description**: * Goes over a {@link WayangPlan} and isolates its loops.

- **Extends**: OneTimeExecutable

### Private Methods:

- **run()**: Returns `void`

---

## LoopSubplan

**Description**: * Wraps a loop of {@link Operator}s.
 *
 * @see LoopIsolator

- **Extends**: Subplan

### Fields:

- **loopHead**: `LoopHeadOperator` (private)

### Public Methods:

- **getNumExpectedIterations()**: Returns `int`
- **getLoopHead()**: Returns `LoopHeadOperator`
- **getInnerInputOptimizationContext()**: Returns `Collection<OptimizationContext>`
- **getInnerOutputOptimizationContext()**: Returns `OptimizationContext`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **noteReplaced()**: Returns `void`

---

## Operator

---

## OperatorAlternative

**Description**: * This operator encapsulates operators that are alternative to each other.
 * <p>Alternatives and their interfaces (i.e., {@link OutputSlot}s and {@link InputSlot}s) are matched via their
 * input/output indices.</p>

- **Extends**: OperatorBase
- **Implements**: CompositeOperator

### Public Methods:

- **getAlternatives()**: Returns `List<Alternative>`
- **addAlternative()**: Returns `Alternative`
- **noteReplaced()**: Returns `void`
- **propagateOutputCardinality()**: Returns `void`
- **propagateInputCardinality()**: Returns `void`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **getContainers()**: Returns `Collection<OperatorContainer>`
- **toString()**: Returns `String`
- **getSlotMapping()**: Returns `SlotMapping`
- **getSink()**: Returns `Operator`
- **setSink()**: Returns `void`
- **toOperator()**: Returns `OperatorAlternative`
- **getSource()**: Returns `Operator`
- **setSource()**: Returns `void`
- **getOperatorAlternative()**: Returns `OperatorAlternative`
- **toString()**: Returns `String`

---

## OperatorBase
- **Implements**: Operator

### Fields:

- **container**: `OperatorContainer` (private)
- **original**: `ExecutionOperator` (private)
- **name**: `String` (private)

### Public Methods:

- **isSupportingBroadcastInputs()**: Returns `boolean`
- **addBroadcastInput()**: Returns `int`
- **getContainer()**: Returns `OperatorContainer`
- **setContainer()**: Returns `void`
- **getEpoch()**: Returns `int`
- **setEpoch()**: Returns `void`
- **at()**: Returns `Operator`
- **toString()**: Returns `String`
- **getTargetPlatforms()**: Returns `Set<Platform>`
- **addTargetPlatform()**: Returns `void`
- **propagateOutputCardinality()**: Returns `void`
- **propagateInputCardinality()**: Returns `void`
- **copy()**: Returns `ExecutionOperator`
- **getOriginal()**: Returns `ExecutionOperator`
- **getName()**: Returns `String`
- **setName()**: Returns `void`
- **getCardinalityEstimator()**: Returns `CardinalityEstimator`
- **setCardinalityEstimator()**: Returns `void`
- **isAuxiliary()**: Returns `boolean`
- **setAuxiliary()**: Returns `void`
- **serialize()**: Returns `JsonElement`
- **deserialize()**: Returns `Operator`

---

## OperatorContainer
- **Extends**: InputSlot

---

## OperatorContainers

**Description**: * Utilities to deal with {@link OperatorContainer}s.


---

## Operators

**Description**: * Utility class for {@link Operator}s.


---

## OutputSlot

**Description**: * An output slot declares an output of an {@link Operator}.

- **Extends**: Slot

### Public Methods:

- **stealOccupiedSlots()**: Returns `void`
- **getIndex()**: Returns `int`
- **copyFor()**: Returns `OutputSlot`
- **connectTo()**: Returns `void`
- **disconnectFrom()**: Returns `void`
- **getOccupiedSlots()**: Returns `List<InputSlot<T>>`
- **unchecked()**: Returns `OutputSlot<Object>`
- **collectRelatedSlots()**: Returns `Set<OutputSlot<T>>`
- **isFeedforward()**: Returns `boolean`

---

## PlanMetrics
- **Extends**: Measurement

### Fields:

- **numCombinations**: `long` (private)

### Public Methods:

- **getNumVirtualOperators()**: Returns `int`
- **getNumExecutionOperators()**: Returns `int`
- **getNumAlternatives()**: Returns `int`
- **getNumCombinations()**: Returns `long`

### Private Methods:

- **collectFrom()**: Returns `void`
- **collectFrom()**: Returns `long`
- **collectFrom()**: Returns `long`

---

## PlanTraversal

**Description**: * Traverse a plan. In each instance, every operator will be traversed only once.

- **Extends**: Operator

### Public Methods:

- **withCallback()**: Returns `PlanTraversal`
- **withCallback()**: Returns `PlanTraversal`
- **followingInputsIf()**: Returns `PlanTraversal`
- **followingInputsDownstreamIf()**: Returns `PlanTraversal`
- **followingOutputsIf()**: Returns `PlanTraversal`
- **enteringContainersIf()**: Returns `PlanTraversal`
- **consideringEnteredOperatorsIf()**: Returns `PlanTraversal`
- **traversingHierarchically()**: Returns `PlanTraversal`
- **traversingFlat()**: Returns `PlanTraversal`
- **traverse()**: Returns `PlanTraversal`
- **traverse()**: Returns `PlanTraversal`
- **traverse()**: Returns `PlanTraversal`
- **traverseFocused()**: Returns `PlanTraversal`
- **traverse()**: Returns `PlanTraversal`
- **getTraversedNodesWith()**: Returns `Collection<Operator>`
- **getTraversedNodes()**: Returns `Collection<Operator>`

### Private Methods:

- **visit()**: Returns `boolean`
- **traverseHierarchical()**: Returns `boolean`
- **enter()**: Returns `void`
- **followOutputs()**: Returns `void`

---

## Slot
- **Extends**: Slot

### Public Methods:

- **getName()**: Returns `String`
- **getOwner()**: Returns `Operator`
- **getType()**: Returns `DataSetType<T>`
- **isOutputSlot()**: Returns `boolean`
- **isInputSlot()**: Returns `boolean`
- **isCompatibleWith()**: Returns `boolean`
- **toString()**: Returns `String`
- **setCardinalityEstimate()**: Returns `void`
- **getCardinalityEstimate()**: Returns `CardinalityEstimate`
- **mark()**: Returns `void`
- **getAndClearMark()**: Returns `boolean`
- **isMarked()**: Returns `boolean`

---

## SlotMapping

**Description**: * This mapping can be used to encapsulate subplans by connecting slots (usually <b>against</b> the data flow direction,
 * i.e., outer output slot to inner output slot, inner input slot to outer input slot).


### Public Methods:

- **mapAllUpsteam()**: Returns `void`
- **mapAllUpsteam()**: Returns `void`
- **mapUpstream()**: Returns `void`
- **mapUpstream()**: Returns `void`
- **replaceInputSlotMappings()**: Returns `void`
- **replaceOutputSlotMappings()**: Returns `void`

### Private Methods:

- **delete()**: Returns `void`
- **delete()**: Returns `void`

---

## Subplan

**Description**: * A subplan encapsulates connected operators as a single operator.

- **Extends**: OperatorBase
- **Implements**: ActualOperator, CompositeOperator, OperatorContainer

### Public Methods:

- **getSlotMapping()**: Returns `SlotMapping`
- **setSource()**: Returns `void`
- **getSource()**: Returns `Operator`
- **isSource()**: Returns `boolean`
- **isSink()**: Returns `boolean`
- **setSink()**: Returns `void`
- **getSink()**: Returns `Operator`
- **toOperator()**: Returns `CompositeOperator`
- **noteReplaced()**: Returns `void`
- **collectOutputOperators()**: Returns `Collection<Operator>`
- **collectInputOperators()**: Returns `Collection<Operator>`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **propagateInputCardinality()**: Returns `void`
- **propagateOutputCardinality()**: Returns `void`
- **getContainers()**: Returns `Collection<OperatorContainer>`

---

## TopDownPlanVisitor

### Public Methods:

- **process()**: Returns `Return`
- **visit()**: Returns `Return`

---

## UnarySink
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator

### Public Methods:

- **getInput()**: Returns `InputSlot<T>`
- **getType()**: Returns `DataSetType<T>`

---

## UnarySource
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator

### Public Methods:

- **getOutput()**: Returns `OutputSlot<T>`
- **getType()**: Returns `DataSetType<T>`

---

## UnaryToUnaryOperator
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator

### Public Methods:

- **getInput()**: Returns `InputSlot<InputType>`
- **getOutput()**: Returns `OutputSlot<OutputType>`
- **getInputType()**: Returns `DataSetType<InputType>`
- **getOutputType()**: Returns `DataSetType<OutputType>`

---

## WayangPlan

**Description**: * A Wayang plan consists of a set of {@link Operator}s.


### Public Methods:

- **prepare()**: Returns `void`
- **addSink()**: Returns `void`
- **replaceSink()**: Returns `void`
- **getSinks()**: Returns `Collection<Operator>`
- **collectReachableTopLevelSources()**: Returns `Collection<Operator>`
- **collectTopLevelOperatorsByName()**: Returns `Collection<Operator>`
- **collectTopLevelOperatorByName()**: Returns `Operator`
- **prune()**: Returns `void`
- **applyTransformations()**: Returns `void`
- **isSane()**: Returns `boolean`
- **isLoopsIsolated()**: Returns `boolean`
- **setLoopsIsolated()**: Returns `void`

### Private Methods:

- **pruneUnreachableSuccessors()**: Returns `void`
- **applyAndCountTransformations()**: Returns `int`

---

## AbstractTopologicalTraversal
- **Extends**: AbstractTopologicalTraversal

### Public Methods:

- **toString()**: Returns `String`

---

## AbstractChannelInstance
- **Extends**: ExecutionResourceTemplate
- **Implements**: ChannelInstance

### Fields:

- **lineage**: `ChannelLineageNode` (private)

### Public Methods:

- **getMeasuredCardinality()**: Returns `OptionalLong`
- **setMeasuredCardinality()**: Returns `void`
- **getLineage()**: Returns `ChannelLineageNode`
- **wasProduced()**: Returns `boolean`
- **markProduced()**: Returns `void`
- **toString()**: Returns `String`

---

## AtomicExecution

**Description**: * An atomic execution describes the smallest work unit considered by Wayang's cost model.

- **Extends**: AtomicExecution
- **Implements**: JsonSerializer

### Fields:

- **loadProfileEstimator**: `LoadProfileEstimator` (private)

### Public Methods:

- **estimateLoad()**: Returns `LoadProfile`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `AtomicExecution`
- **deserialize()**: Returns `AtomicExecution`
- **getLoadProfileEstimator()**: Returns `LoadProfileEstimator`
- **setLoadProfileEstimator()**: Returns `void`
- **toString()**: Returns `String`

### Private Methods:

- **serialize()**: Returns `void`
- **deserializeEstimator()**: Returns `LoadProfileEstimator`

---

## AtomicExecutionGroup

**Description**: * This class groups {@link AtomicExecution}s with a common {@link EstimationContext} and {@link Platform}.

- **Extends**: AtomicExecutionGroup
- **Implements**: JsonSerializer

### Fields:

- **estimationContext**: `EstimationContext` (private)
- **platform**: `Platform` (private)
- **atomicExecutions**: `Collection<AtomicExecution>` (private)
- **configuration**: `Configuration` (private)
- **loadProfileToTimeConverterCache**: `LoadProfileToTimeConverter` (private)

### Public Methods:

- **estimateLoad()**: Returns `LoadProfile`
- **estimateLoad()**: Returns `LoadProfile`
- **estimateExecutionTime()**: Returns `TimeEstimate`
- **estimateExecutionTime()**: Returns `TimeEstimate`
- **getEstimationContext()**: Returns `EstimationContext`
- **getPlatform()**: Returns `Platform`
- **getAtomicExecutions()**: Returns `Collection<AtomicExecution>`
- **toString()**: Returns `String`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `AtomicExecutionGroup`

---

## Breakpoint

---

## CardinalityBreakpoint

**Description**: * {@link Breakpoint} implementation that is based on the {@link CardinalityEstimate}s of {@link Channel}s.
 * <p>Specifically, this implementation requires that <i>all</i> {@link CardinalityEstimate}s of the inbound
 * {@link Channel}s of an {@link ExecutionStage} are to a certain extent accurate within a given probability.</p>

- **Implements**: Breakpoint

### Public Methods:

- **permitsExecutionOf()**: Returns `boolean`
- **approves()**: Returns `boolean`
- **calculateSpread()**: Returns `double`

### Private Methods:

- **getCardinalityEstimate()**: Returns `CardinalityEstimate`

---

## ChannelDescriptor

**Description**: * Describes a certain {@link Channel} type including further parameters.

- **Extends**: Channel

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **isReusable()**: Returns `boolean`
- **isSuitableForBreakpoint()**: Returns `boolean`
- **createChannel()**: Returns `Channel`

---

## ChannelInstance
- **Extends**: ExecutionResource

---

## CompositeExecutionResource
- **Extends**: ExecutionResource

---

## ConjunctiveBreakpoint

**Description**: * {@link Breakpoint} implementation that disrupts execution if all aggregated {@link Breakpoint}s request a disruption.
 * However, if no {@link Breakpoint} conjuncts are set up, then it will never break.

- **Implements**: Breakpoint

### Public Methods:

- **permitsExecutionOf()**: Returns `boolean`
- **addConjunct()**: Returns `void`

---

## CrossPlatformExecutor

**Description**: * Executes a (cross-platform) {@link ExecutionPlan}.

- **Extends**: AbstractReferenceCountable
- **Implements**: ExecutionState

### Fields:

- **threadId**: `String` (public)
- **thread_isBreakpointDisabled**: `boolean` (private)

### Public Methods:

- **executeUntilBreakpoint()**: Returns `boolean`
- **prepare()**: Returns `void`
- **getChannelInstance()**: Returns `ChannelInstance`
- **getChannelInstance()**: Returns `ChannelInstance`
- **register()**: Returns `void`
- **registerGlobal()**: Returns `void`
- **addCardinalityMeasurement()**: Returns `void`
- **getCardinalityMeasurements()**: Returns `Collection<ChannelInstance>`
- **add()**: Returns `void`
- **getPartialExecutions()**: Returns `Collection<PartialExecution>`
- **setBreakpoint()**: Returns `void`
- **isVetoingPlanChanges()**: Returns `boolean`
- **shutdown()**: Returns `void`
- **getCompletedStages()**: Returns `Set<ExecutionStage>`
- **getConfiguration()**: Returns `Configuration`
- **getStage()**: Returns `ExecutionStage`
- **getOptimizationContext()**: Returns `OptimizationContext`
- **noteActivation()**: Returns `void`
- **activateNextIteration()**: Returns `void`
- **getOrCreateNextTransition()**: Returns `ExecutionStageLoopIterationContext`
- **isDisposed()**: Returns `boolean`
- **register()**: Returns `void`
- **getChannelInstance()**: Returns `ChannelInstance`
- **scrapPreviousTransitionContext()**: Returns `void`
- **register()**: Returns `void`
- **getChannelInstance()**: Returns `ChannelInstance`
- **run()**: Returns `void`

### Private Methods:

- **determineInitialOptimizationContext()**: Returns `OptimizationContext`
- **getOrCreateActivator()**: Returns `StageActivator`
- **tryToActivate()**: Returns `boolean`
- **executeSingleStage()**: Returns `void`
- **runParallelExecution()**: Returns `void`
- **runToBreakpoint()**: Returns `void`
- **suspendIfBreakpointRequest()**: Returns `boolean`
- **execute()**: Returns `void`
- **getOrCreateExecutorFor()**: Returns `Executor`
- **tryToActivateSuccessors()**: Returns `void`
- **determineNextOptimizationContext()**: Returns `OptimizationContext`
- **getOrCreateLoopContext()**: Returns `ExecutionStageLoopContext`
- **removeLoopContext()**: Returns `void`
- **checkIfIsLoopInput()**: Returns `boolean`
- **updateChannelInstances()**: Returns `boolean`
- **createIterationContext()**: Returns `ExecutionStageLoopIterationContext`

---

## ExecutionResource
- **Extends**: ReferenceCountable

---

## ExecutionResourceTemplate
- **Extends**: AbstractReferenceCountable
- **Implements**: ExecutionResource

### Public Methods:

- **dispose()**: Returns `void`

---

## ExecutionState

---

## Executor
- **Extends**: CompositeExecutionResource

---

## ExecutorTemplate
- **Extends**: AbstractReferenceCountable
- **Implements**: Executor

### Public Methods:

- **register()**: Returns `void`
- **unregister()**: Returns `void`
- **dispose()**: Returns `void`
- **getCrossPlatformExecutor()**: Returns `CrossPlatformExecutor`
- **toString()**: Returns `String`
- **getConfiguration()**: Returns `Configuration`

---

## FixBreakpoint

**Description**: * Describes when to interrupt the execution of an {@link ExecutionPlan}.

- **Implements**: Breakpoint

### Public Methods:

- **breakAfter()**: Returns `FixBreakpoint`
- **breakBefore()**: Returns `FixBreakpoint`
- **breakBefore()**: Returns `FixBreakpoint`
- **permitsExecutionOf()**: Returns `boolean`

---

## Junction

**Description**: * Describes the implementation of one {@link OutputSlot} to its occupied {@link InputSlot}s.


### Fields:

- **sourceChannel**: `Channel` (private)

### Public Methods:

- **getSourceOperator()**: Returns `ExecutionOperator`
- **getTargetOperator()**: Returns `ExecutionOperator`
- **getSourceChannel()**: Returns `Channel`
- **setSourceChannel()**: Returns `void`
- **getTargetChannels()**: Returns `List<Channel>`
- **getTargetChannel()**: Returns `Channel`
- **setTargetChannel()**: Returns `void`
- **getNumTargets()**: Returns `int`
- **getConversionTasks()**: Returns `Collection<ExecutionTask>`
- **getTimeEstimate()**: Returns `TimeEstimate`
- **getCostEstimate()**: Returns `ProbabilisticDoubleInterval`
- **getSquashedCostEstimate()**: Returns `double`
- **getOverallTimeEstimate()**: Returns `TimeEstimate`
- **toString()**: Returns `String`
- **register()**: Returns `void`
- **getOptimizationContexts()**: Returns `List<OptimizationContext>`

### Private Methods:

- **findMatchingOptimizationContext()**: Returns `OptimizationContext`

---

## NoIterationBreakpoint

**Description**: * This {@link Breakpoint} implementation always requests a break unless inside of {@link ExecutionStageLoop}s.

- **Implements**: Breakpoint

### Public Methods:

- **permitsExecutionOf()**: Returns `boolean`

---

## PartialExecution

**Description**: * Captures data of a execution of a set of {@link ExecutionOperator}s.

- **Extends**: PartialExecution
- **Implements**: JsonSerializer

### Public Methods:

- **getMeasuredExecutionTime()**: Returns `long`
- **getMeasuredLowerCost()**: Returns `double`
- **getMeasuredUpperCost()**: Returns `double`
- **getInvolvedPlatforms()**: Returns `Set<Platform>`
- **getOverallTimeEstimate()**: Returns `TimeEstimate`
- **getInitializedPlatforms()**: Returns `Collection<Platform>`
- **addInitializedPlatform()**: Returns `void`
- **getAtomicExecutionGroups()**: Returns `Collection<AtomicExecutionGroup>`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `PartialExecution`

---

## Platform
- **Extends**: Platform

### Public Methods:

- **createExecutor()**: Returns `Executor`
- **getName()**: Returns `String`
- **getConfigurationName()**: Returns `String`
- **toString()**: Returns `String`
- **isSinglePlatformExecutionPossible()**: Returns `boolean`
- **warmUp()**: Returns `void`
- **getInitializeMillis()**: Returns `long`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `Platform`

---

## PushExecutorTemplate
- **Extends**: ExecutorTemplate

### Public Methods:

- **execute()**: Returns `void`
- **accept()**: Returns `void`
- **getTask()**: Returns `ExecutionTask`
- **getJob()**: Returns `Job`

### Private Methods:

- **scheduleStartTask()**: Returns `void`
- **store()**: Returns `void`
- **activateSuccessorTasks()**: Returns `void`
- **executor()**: Returns `PushExecutorTemplate`
- **updateExecutionState()**: Returns `void`
- **acceptFrom()**: Returns `void`

---

## ChannelLineageNode

**Description**: * Encapsulates a {@link ChannelInstance} in the lazy execution lineage.

- **Extends**: LazyExecutionLineageNode

### Public Methods:

- **toString()**: Returns `String`
- **getChannelInstance()**: Returns `ChannelInstance`

---

## ExecutionLineageNode

**Description**: * Encapsulates {@link AtomicExecution}s with a common {@link OptimizationContext.OperatorContext} in a lazy execution lineage.

- **Extends**: LazyExecutionLineageNode

### Public Methods:

- **add()**: Returns `ExecutionLineageNode`
- **add()**: Returns `ExecutionLineageNode`
- **addAtomicExecutionFromOperatorContext()**: Returns `ExecutionLineageNode`
- **getAtomicExecutions()**: Returns `Collection<AtomicExecution>`
- **toString()**: Returns `String`

---

## LazyExecutionLineageNode
- **Implements**: Aggregator

### Public Methods:

- **addPredecessor()**: Returns `void`

---

## DynamicPlugin

**Description**: * This {@link Plugin} can be arbitrarily customized.

- **Implements**: Plugin

### Public Methods:

- **addRequiredPlatform()**: Returns `void`
- **excludeRequiredPlatform()**: Returns `void`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getExcludedRequiredPlatforms()**: Returns `Collection<Platform>`
- **addMapping()**: Returns `void`
- **excludeMapping()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getExcludedMappings()**: Returns `Collection<Mapping>`
- **addChannelConversion()**: Returns `void`
- **excludeChannelConversion()**: Returns `void`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getExcludedChannelConversions()**: Returns `Collection<ChannelConversion>`
- **addProperty()**: Returns `void`
- **setProperties()**: Returns `void`

---

## Plugin

---

## CardinalityRepository

**Description**: * Stores cardinalities that have been collected by the {@link CrossPlatformExecutor}. Current version uses
 * JSON as serialization format.


### Fields:

- **writer**: `BufferedWriter` (private)

### Public Methods:

- **storeAll()**: Returns `void`
- **store()**: Returns `void`
- **sleep()**: Returns `void`

### Private Methods:

- **write()**: Returns `void`
- **write()**: Returns `void`
- **getWriter()**: Returns `BufferedWriter`

---

## CostMeasurement
- **Extends**: Measurement

### Fields:

- **probability**: `double` (private)

### Public Methods:

- **getLowerCost()**: Returns `double`
- **setLowerCost()**: Returns `void`
- **getUpperCost()**: Returns `double`
- **setUpperCost()**: Returns `void`
- **getProbability()**: Returns `double`
- **setProbability()**: Returns `void`

---

## ExecutionLog

**Description**: * Stores execution data have been collected by the {@link CrossPlatformExecutor}.
 * The current version uses JSON as serialization format.

- **Implements**: AutoCloseable

### Fields:

- **writer**: `BufferedWriter` (private)

### Public Methods:

- **storeAll()**: Returns `void`
- **store()**: Returns `void`
- **stream()**: Returns `Stream<PartialExecution>`
- **close()**: Returns `void`

### Private Methods:

- **store()**: Returns `void`
- **write()**: Returns `void`
- **getWriter()**: Returns `BufferedWriter`

---

## ExecutionPlanMeasurement
- **Extends**: Measurement

### Fields:

- **channels**: `List<ChannelNode>` (private)
- **operators**: `List<OperatorNode>` (private)
- **links**: `List<Link>` (private)
- **id**: `int` (private)
- **type**: `String` (private)
- **dataQuantaType**: `String` (private)
- **id**: `int` (private)
- **type**: `String` (private)
- **name**: `String` (private)
- **platform**: `String` (private)

### Public Methods:

- **getChannels()**: Returns `List<ChannelNode>`
- **getOperators()**: Returns `List<OperatorNode>`
- **getLinks()**: Returns `List<Link>`
- **getType()**: Returns `String`
- **setType()**: Returns `void`
- **getDataQuantaType()**: Returns `String`
- **setDataQuantaType()**: Returns `void`
- **getId()**: Returns `int`
- **setId()**: Returns `void`
- **getType()**: Returns `String`
- **setType()**: Returns `void`
- **getId()**: Returns `int`
- **setId()**: Returns `void`
- **getName()**: Returns `String`
- **setName()**: Returns `void`
- **getPlatform()**: Returns `String`
- **setPlatform()**: Returns `void`
- **getSource()**: Returns `int`
- **setSource()**: Returns `void`
- **getDestination()**: Returns `int`
- **setDestination()**: Returns `void`

---

## FullInstrumentationStrategy

**Description**: * Instruments only outbound {@link Channel}s.

- **Implements**: InstrumentationStrategy

### Public Methods:

- **applyTo()**: Returns `void`

---

## InstrumentationStrategy

---

## NoInstrumentationStrategy
- **Implements**: InstrumentationStrategy

### Public Methods:

- **applyTo()**: Returns `void`

---

## OutboundInstrumentationStrategy

**Description**: * Instruments only outbound {@link Channel}s.

- **Implements**: InstrumentationStrategy

### Public Methods:

- **applyTo()**: Returns `void`

---

## PartialExecutionMeasurement
- **Extends**: Measurement

### Fields:

- **executionMillis**: `long` (private)
- **estimatedExecutionMillis**: `TimeEstimate` (private)

### Public Methods:

- **getExecutionMillis()**: Returns `long`
- **setExecutionMillis()**: Returns `void`
- **getEstimatedExecutionMillis()**: Returns `TimeEstimate`
- **setEstimatedExecutionMillis()**: Returns `void`

---

## ProfileDBs

**Description**: * Utilities to work with {@link ProfileDB}s.


---

## BasicDataUnitType

**Description**: * A basic data unit type is elementary and not constructed from other data unit types.

- **Extends**: DataUnitType

### Public Methods:

- **isGroup()**: Returns `boolean`
- **toBasicDataUnitType()**: Returns `BasicDataUnitType<T>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getTypeClass()**: Returns `Class<T>`

---

## DataSetType

**Description**: * A data set is an abstraction of the Wayang programming model. Although never directly materialized, a data set
 * keeps track of type and structure of data units being passed between operators.

- **Extends**: T

### Public Methods:

- **getDataUnitType()**: Returns `DataUnitType<T>`
- **unchecked()**: Returns `DataSetType<Object>`
- **uncheckedGroup()**: Returns `DataSetType<Iterable<Object>>`
- **isSupertypeOf()**: Returns `boolean`
- **isNone()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

---

## DataUnitGroupType

**Description**: * A grouped data unit type describes just the structure of data units within a grouped dataset.

- **Extends**: DataUnitType

### Public Methods:

- **isGroup()**: Returns `boolean`
- **getTypeClass()**: Returns `Class<Iterable<T>>`
- **getBaseType()**: Returns `DataUnitType<T>`
- **toBasicDataUnitType()**: Returns `BasicDataUnitType<Iterable<T>>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

---

## DataUnitType

### Public Methods:

- **isPlain()**: Returns `boolean`
- **isSupertypeOf()**: Returns `boolean`

---

## AbstractReferenceCountable
- **Implements**: ReferenceCountable

### Public Methods:

- **disposeIfUnreferenced()**: Returns `boolean`
- **getNumReferences()**: Returns `int`
- **noteObtainedReference()**: Returns `void`
- **noteDiscardedReference()**: Returns `void`
- **isDisposed()**: Returns `boolean`

---

## Action

---

## Actions

**Description**: * Utilities to perform actions.


---

## Bitmask

**Description**: * A mutable bit-mask.

- **Implements**: Cloneable, Iterable

### Fields:

- **cardinalityCache**: `int` (private)

### Public Methods:

- **set()**: Returns `boolean`
- **get()**: Returns `boolean`
- **cardinality()**: Returns `int`
- **isEmpty()**: Returns `boolean`
- **orInPlace()**: Returns `Bitmask`
- **or()**: Returns `Bitmask`
- **andInPlace()**: Returns `Bitmask`
- **and()**: Returns `Bitmask`
- **andNotInPlace()**: Returns `Bitmask`
- **andNot()**: Returns `Bitmask`
- **flip()**: Returns `Bitmask`
- **isSubmaskOf()**: Returns `boolean`
- **isDisjointFrom()**: Returns `boolean`
- **nextSetBit()**: Returns `int`
- **hasNext()**: Returns `boolean`
- **nextInt()**: Returns `int`
- **next()**: Returns `Integer`
- **stream()**: Returns `IntStream`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Private Methods:

- **ensureCapacity()**: Returns `boolean`
- **toHexString()**: Returns `String`
- **toBinString()**: Returns `String`
- **toIndexString()**: Returns `String`

---

## Canonicalizer

**Description**: * This utility maintains canonical sets of objects.

- **Extends**: T
- **Implements**: Set

### Public Methods:

- **getOrAdd()**: Returns `T`
- **addAll()**: Returns `void`
- **size()**: Returns `int`
- **isEmpty()**: Returns `boolean`
- **contains()**: Returns `boolean`
- **iterator()**: Returns `Iterator<T>`
- **add()**: Returns `boolean`
- **remove()**: Returns `boolean`
- **containsAll()**: Returns `boolean`
- **addAll()**: Returns `boolean`
- **retainAll()**: Returns `boolean`
- **removeAll()**: Returns `boolean`
- **clear()**: Returns `void`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

## ConsumerIteratorAdapter

**Description**: * Utility to expose interfaces that accept a callback as an {@link Iterator}.
 * <p>This class uses a lock-free ring buffer to achieve a high throughput and minimize stall times. Note that
 * the producer and consumer should run in different threads, otherwise deadlocks might occur.</p>
 * <p>The producer obtains a {@link Consumer} via {@link #getConsumer()} and pushes elements to it. When all
 * elements are pushed, {@link #declareLastAdd()} should be called. The consumer obtains a {@link Iterator} via
 * {@link #getIterator()} from that previously pushed elements can be obtained. Both operators can block when the
 * buffer is full or empty.</p>


### Fields:

- **next**: `T` (private)

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `T`
- **getIterator()**: Returns `Iterator<T>`
- **getConsumer()**: Returns `Consumer<T>`
- **declareLastAdd()**: Returns `void`

### Private Methods:

- **ensureInitialized()**: Returns `void`
- **moveToNext()**: Returns `void`
- **add()**: Returns `void`
- **read()**: Returns `T`

---

## Copyable

---

## Counter

**Description**: * This utility helps to count elements.

- **Implements**: Iterable

### Public Methods:

- **get()**: Returns `int`
- **add()**: Returns `int`
- **increment()**: Returns `int`
- **decrement()**: Returns `int`
- **addAll()**: Returns `void`
- **isEmpty()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **remove()**: Returns `void`
- **clear()**: Returns `void`

### Private Methods:

- **addAll()**: Returns `void`

---

## CrossProductIterable

**Description**: * Iterates all combinations, i.e., the Cartesian product, of given {@link Iterable}s.

- **Extends**: Iterable
- **Implements**: Iterable

### Fields:

- **vals**: `List<T>` (private)
- **hasEmptyIterator**: `boolean` (private)

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `List<T>`

---

## Formats

**Description**: * Formats different general purpose objects.


---

## Iterators

**Description**: * Utilities for the work with {@link Iterator}s.


---

## JsonSerializable
- **Extends**: JsonSerializable
- **Implements**: JsonSerializer

### Public Methods:

- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `T`

---

## JsonSerializables

**Description**: * Utility to deal with {@link JsonSerializable}s.

- **Extends**: T

---

## JsonSerializer
- **Extends**: T

---

## JuelUtils

**Description**: * Utilities to deal with JUEL expressions.


### Public Methods:

- **apply()**: Returns `T`
- **apply()**: Returns `T`

### Private Methods:

- **initializeContext()**: Returns `void`

---

## LimitedInputStream

**Description**: * {@link InputStream} that is trimmed to a specified size. Moreover, it counts the number of read bytes.

- **Extends**: InputStream

### Public Methods:

- **read()**: Returns `int`
- **read()**: Returns `int`
- **skip()**: Returns `long`
- **available()**: Returns `int`
- **close()**: Returns `void`
- **markSupported()**: Returns `boolean`
- **getNumReadBytes()**: Returns `long`

### Private Methods:

- **getMaxBytesToRead()**: Returns `int`

---

## Logging

---

## LruCache

**Description**: * Key-value cache with "least recently used" eviction strategy.

- **Extends**: LinkedHashMap

### Public Methods:

- **getCapacity()**: Returns `int`

---

## MultiMap

**Description**: * Maps keys to multiple values. Each key value pair is unique.

- **Extends**: HashMap

### Public Methods:

- **putSingle()**: Returns `boolean`
- **removeSingle()**: Returns `boolean`

---

## OneTimeExecutable

---

## Optional

### Public Methods:

- **isAvailable()**: Returns `boolean`
- **getValue()**: Returns `Object`
- **stream()**: Returns `Stream`
- **isAvailable()**: Returns `boolean`
- **getValue()**: Returns `T`
- **stream()**: Returns `Stream<T>`

---

## ReferenceCountable

---

## ReflectionUtils

**Description**: * Utilities for reflection code.

- **Extends**: T

---

## Tuple

**Description**: * A helper data structure to manage two values without creating new classes.


### Fields:

- **field0**: `T0` (public)
- **field1**: `T1` (public)

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **setField0()**: Returns `void`
- **getField1()**: Returns `T1`
- **setField1()**: Returns `void`

---

## WayangArrays

**Description**: * Utility for handling arrays.


---

## WayangCollections

**Description**: * Utilities to operate {@link java.util.Collection}s.

- **Extends**: Collection

---

## FileSystem

---

## FileSystems

**Description**: * Tool to work with {@link FileSystem}s.


---

## FileUtils

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `String`

### Private Methods:

- **advance()**: Returns `void`

---

## HadoopFileSystem

**Description**: * {@link FileSystem} immplementation for the HDFS.

- **Implements**: FileSystem

### Public Methods:

- **ensureInitialized()**: Returns `void`
- **getFileSize()**: Returns `long`
- **canHandle()**: Returns `boolean`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

---

## LocalFileSystem

**Description**: * {@link FileSystem} implementation for the local file system.

- **Implements**: FileSystem

### Public Methods:

- **getFileSize()**: Returns `long`
- **canHandle()**: Returns `boolean`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

### Private Methods:

- **delete()**: Returns `boolean`

---

## S3FileSystem
- **Implements**: FileSystem

### Fields:

- **s3**: `AmazonS3` (private)

### Public Methods:

- **getBucket()**: Returns `String`
- **getKey()**: Returns `String`
- **getFileSize()**: Returns `long`
- **canHandle()**: Returns `boolean`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **run()**: Returns `void`
- **bucketExits()**: Returns `boolean`
- **preFoldersExits()**: Returns `boolean`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

### Private Methods:

- **getS3Client()**: Returns `AmazonS3`
- **getS3Pair()**: Returns `S3Pair`
- **getFileSize()**: Returns `long`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

---

## WayangJsonArray

**Description**: * JSONArray is the wrapper for the {@link ArrayNode} to enable the
 * easy access to Json data
 *
 * TODO: the java doc is not done because is missing implementation and it performed some
 *       modification on the code

- **Implements**: Iterable

### Fields:

- **node**: `ArrayNode` (private)

### Public Methods:

- **length()**: Returns `int`
- **getJSONObject()**: Returns `WayangJsonObj`
- **put()**: Returns `void`
- **put()**: Returns `void`
- **toString()**: Returns `String`
- **iterator()**: Returns `Iterator<Object>`

---

## WayangJsonObj

**Description**: * JSONObject is the wrapper for the {@link ObjectNode} to enable the
 * easy access to the json data
 *
 * TODO: the java doc is not done because is missing implementation and it performed some
 *       modification on the code


### Fields:

- **node**: `ObjectNode` (private)

### Public Methods:

- **getNode()**: Returns `ObjectNode`
- **has()**: Returns `boolean`
- **get()**: Returns `String`
- **getString()**: Returns `String`
- **getDouble()**: Returns `double`
- **getLong()**: Returns `long`
- **getInt()**: Returns `int`
- **getJSONObject()**: Returns `WayangJsonObj`
- **getJSONArray()**: Returns `WayangJsonArray`
- **put()**: Returns `WayangJsonObj`
- **put()**: Returns `WayangJsonObj`
- **put()**: Returns `WayangJsonObj`
- **put()**: Returns `WayangJsonObj`
- **put()**: Returns `WayangJsonObj`
- **write()**: Returns `void`
- **put()**: Returns `WayangJsonObj`
- **put()**: Returns `WayangJsonObj`
- **putOptional()**: Returns `WayangJsonObj`
- **optionalWayangJsonObj()**: Returns `WayangJsonObj`
- **optionalWayangJsonArray()**: Returns `WayangJsonArray`
- **optionalDouble()**: Returns `double`
- **optionalDouble()**: Returns `double`
- **keySet()**: Returns `Set<String>`
- **length()**: Returns `int`
- **toString()**: Returns `String`

---

## Context

---

## DefaultContext

**Description**: * Default {@link Context} implementation that can be configured.

- **Implements**: Context

### Fields:

- **parentContext**: `Context` (private)

### Public Methods:

- **getVariable()**: Returns `double`
- **setParentContext()**: Returns `void`
- **setVariable()**: Returns `void`
- **setFunction()**: Returns `void`

---

## Expression

---

## ExpressionBuilder

**Description**: * This utility builds {@link Expression}s from an input {@link String}.

- **Extends**: MathExBaseVisitor

### Public Methods:

- **syntaxError()**: Returns `void`
- **syntaxError()**: Returns `void`
- **visitConstant()**: Returns `Expression`
- **visitFunction()**: Returns `Expression`
- **visitVariable()**: Returns `Expression`
- **visitParensExpression()**: Returns `Expression`
- **visitBinaryOperation()**: Returns `Expression`
- **visitUnaryOperation()**: Returns `Expression`

---

## EvaluationException

**Description**: * This exception signals a failed {@link Expression} evaluation.
 *
 * @see Expression#evaluate(Context)

- **Extends**: MathExException

---

## MathExException

**Description**: * This exception signals a failed {@link Expression} evaluation.
 *
 * @see Expression#evaluate(Context)

- **Extends**: RuntimeException

---

## ParseException

**Description**: * This exception signals a failed {@link Expression} evaluation.
 *
 * @see Expression#evaluate(Context)

- **Extends**: MathExException

---

## BinaryOperation

**Description**: * An operation {@link Expression}.

- **Implements**: Expression

### Public Methods:

- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

---

## CompiledFunction

**Description**: * {@link Expression} implementation that represents a function with a static implementation.

- **Implements**: Expression

### Public Methods:

- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

---

## Constant

**Description**: * A constant {@link Expression}.

- **Implements**: Expression

### Public Methods:

- **getValue()**: Returns `double`
- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

---

## NamedFunction

**Description**: * {@link Expression} implementation that represents a function that is identified
 * via its name.

- **Implements**: Expression

### Public Methods:

- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

---

## UnaryOperation

**Description**: * An operation {@link Expression}.

- **Implements**: Expression

### Public Methods:

- **evaluate()**: Returns `double`
- **toString()**: Returns `String`

---

## Variable

**Description**: * A variable {@link Expression}

- **Implements**: Expression

### Public Methods:

- **evaluate()**: Returns `double`
- **toString()**: Returns `String`

---

## SlotTest

**Description**: * Test suite for {@link Slot}s.


### Public Methods:

- **testConnectMismatchingSlotFails()**: Returns `void`
- **testConnectMatchingSlots()**: Returns `void`

---

## OperatorPatternTest

**Description**: * Tests for {@link OperatorPattern}.


### Public Methods:

- **testAdditionalTests()**: Returns `void`

---

## PlanTransformationTest

**Description**: * Test suite for the {@link org.apache.wayang.core.mapping.PlanTransformation} class.


### Public Methods:

- **testReplace()**: Returns `void`
- **testIntroduceAlternative()**: Returns `void`
- **testFlatAlternatives()**: Returns `void`

---

## SubplanPatternTest

**Description**: * Test suite for the {@link SubplanPattern}.


### Public Methods:

- **testMatchSinkPattern()**: Returns `void`
- **testMatchSourcePattern()**: Returns `void`
- **testMatchChainedPattern()**: Returns `void`

---

## TestSinkMapping

**Description**: * Dummy {@link Mapping} implementation from {@link TestSink} to {@link TestSink2}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **hashCode()**: Returns `int`
- **equals()**: Returns `boolean`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## TestSinkToTestSink2Factory

**Description**: * This factory replaces a {@link TestSink} by a
 * {@link TestSink2}.

- **Extends**: ReplacementSubplanFactory

---

## AggregatingCardinalityEstimatorTest

**Description**: * Test suite for {@link AggregatingCardinalityEstimator}.


### Public Methods:

- **testEstimate()**: Returns `void`

---

## DefaultCardinalityEstimatorTest

**Description**: * Test suite for the {@link DefaultCardinalityEstimator}.


### Public Methods:

- **testBinaryInputEstimation()**: Returns `void`

---

## LoopSubplanCardinalityPusherTest

**Description**: * Test suite for {@link LoopSubplanCardinalityPusher}.


### Fields:

- **job**: `Job` (private)
- **configuration**: `Configuration` (private)

### Public Methods:

- **setUp()**: Returns `void`
- **testWithSingleLoopAndSingleIteration()**: Returns `void`
- **testWithSingleLoopAndManyIteration()**: Returns `void`
- **testWithSingleLoopWithConstantInput()**: Returns `void`
- **testNestedLoops()**: Returns `void`

---

## SubplanCardinalityPusherTest

**Description**: * Test suite for {@link SubplanCardinalityPusher}.


### Fields:

- **job**: `Job` (private)
- **configuration**: `Configuration` (private)

### Public Methods:

- **setUp()**: Returns `void`
- **testSimpleSubplan()**: Returns `void`
- **testSourceSubplan()**: Returns `void`
- **testDAGShapedSubplan()**: Returns `void`

---

## ChannelConversionGraphTest

**Description**: * Test suite for {@link ChannelConversionGraph}.


### Public Methods:

- **findDirectConversion()**: Returns `void`
- **findIntricateConversion()**: Returns `void`
- **findIntricateConversion2()**: Returns `void`
- **updateExistingConversionWithOnlySourceChannel()**: Returns `void`
- **updateExistingConversionWithReachedDestination()**: Returns `void`
- **updateExistingConversionWithTwoOpenChannels()**: Returns `void`

---

## NestableLoadProfileEstimatorTest

**Description**: * Tests for the {@link NestableLoadProfileEstimator}.

- **Extends**: UnaryToUnaryOperator
- **Implements**: ExecutionOperator

### Public Methods:

- **testFromJuelSpecification()**: Returns `void`
- **testFromMathExSpecification()**: Returns `void`
- **testFromJuelSpecificationWithImport()**: Returns `void`
- **testMathExFromSpecificationWithImport()**: Returns `void`
- **getNumIterations()**: Returns `int`
- **getPlatform()**: Returns `Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## StageAssignmentTraversalTest

**Description**: * Test suite for {@link StageAssignmentTraversal}.


### Public Methods:

- **testCircularPlatformAssignment()**: Returns `void`
- **testZigZag()**: Returns `void`

---

## TestChannel

**Description**: * {@link Channel} implementation that can be used for test purposes.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `Channel`
- **createInstance()**: Returns `ChannelInstance`

---

## LoopIsolatorTest

**Description**: * Test suite for the {@link LoopIsolator}.


### Public Methods:

- **testWithSingleLoop()**: Returns `void`
- **testWithSingleLoopWithConstantInput()**: Returns `void`
- **testNestedLoops()**: Returns `void`

---

## OperatorTest

**Description**: * Test suite for the {@link Operator} class.

- **Extends**: OperatorBase

### Public Methods:

- **getOp1Property()**: Returns `double`
- **getOp2Property()**: Returns `double`
- **testPropertyDetection()**: Returns `void`
- **testPropertyCollection()**: Returns `void`

---

## SlotMappingTest

**Description**: * Test suite for {@link SlotMapping}.


### Public Methods:

- **testSimpleSlotMapping()**: Returns `void`
- **testOverridingSlotMapping()**: Returns `void`
- **testMultiMappings()**: Returns `void`

---

## TestCustomMapOperator

**Description**: * Test operator that exposes map-like behavior. Does not provide a {@link CardinalityEstimator}.

- **Extends**: UnaryToUnaryOperator

---

## TestFilterOperator

**Description**: * Test operator that exposes filter-like behavior.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **isSupportingBroadcastInputs()**: Returns `boolean`
- **getSelectivity()**: Returns `double`
- **setSelectivity()**: Returns `void`

---

## TestJoin

**Description**: * Join-like operator.

- **Extends**: BinaryToUnaryOperator
- **Implements**: ElementaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## TestLoopHead

**Description**: * {@link LoopHeadOperator} implementation for test purposes.

- **Extends**: OperatorBase
- **Implements**: LoopHeadOperator, ElementaryOperator

### Fields:

- **numExpectedIterations**: `int` (private)

### Public Methods:

- **getNumExpectedIterations()**: Returns `int`
- **setNumExpectedIterations()**: Returns `void`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

---

## TestMapOperator

**Description**: * Test operator that exposes map-like behavior.

- **Extends**: UnaryToUnaryOperator

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **isSupportingBroadcastInputs()**: Returns `boolean`

---

## TestSink2

**Description**: * Another dummy sink for testing purposes.

- **Extends**: UnarySink

---

## PartialExecutionTest

**Description**: * Test suites for {@link PartialExecution}s.


### Public Methods:

- **testJsonSerialization()**: Returns `void`

---

## DynamicPluginTest

**Description**: * Test suite for the {@link DynamicPlugin} class.


### Public Methods:

- **testLoadYaml()**: Returns `void`
- **testPartialYaml()**: Returns `void`
- **testEmptyYaml()**: Returns `void`
- **testExclusion()**: Returns `void`

---

## DummyExecutionOperator

**Description**: * Dummy {@link ExecutionOperator} for test purposes.

- **Extends**: OperatorBase
- **Implements**: ExecutionOperator

### Fields:

- **someProperty**: `int` (private)

### Public Methods:

- **getSomeProperty()**: Returns `int`
- **setSomeProperty()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## DummyExternalReusableChannel

**Description**: * Dummy {@link Channel}.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `DummyExternalReusableChannel`
- **createInstance()**: Returns `ChannelInstance`

---

## DummyNonReusableChannel

**Description**: * Dummy {@link Channel}.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `DummyNonReusableChannel`
- **createInstance()**: Returns `ChannelInstance`

---

## DummyPlatform

**Description**: * {@link Platform} implementation for test purposes.

- **Extends**: Platform

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **convert()**: Returns `TimeEstimate`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

---

## DummyReusableChannel

**Description**: * Dummy {@link Channel}.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `Channel`
- **createInstance()**: Returns `ChannelInstance`

---

## MockFactory

**Description**: * Utility to mock Wayang objects.


---

## SerializableDummyExecutionOperator

**Description**: * Dummy {@link ExecutionOperator} for test purposes.

- **Extends**: DummyExecutionOperator
- **Implements**: JsonSerializable

### Public Methods:

- **toJson()**: Returns `WayangJsonObj`

---

## TestDataUnit

**Description**: * A data unit type for test purposes.


---

## TestDataUnit2

**Description**: * Another data unit type for test purposes.


---

## BitmaskTest

**Description**: * Test suite for {@link Bitmask}s.


### Public Methods:

- **testEquals()**: Returns `void`
- **testFlip()**: Returns `void`
- **testIsSubmaskOf()**: Returns `void`
- **testCardinality()**: Returns `void`
- **testOr()**: Returns `void`
- **testAndNot()**: Returns `void`
- **testNextSetBit()**: Returns `void`

### Private Methods:

- **testFlip()**: Returns `void`
- **testSetBits()**: Returns `void`

---

## ConsumerIteratorAdapterTest

**Description**: * Test suite for the {@link ConsumerIteratorAdapter}.


### Public Methods:

- **testCriticalLoad()**: Returns `void`

---

## CrossProductIterableTest

**Description**: * Test suite for {@link CrossProductIterable}.


### Public Methods:

- **test2x3()**: Returns `void`
- **test3x2()**: Returns `void`
- **test1x3()**: Returns `void`
- **test3x1()**: Returns `void`
- **test1x1()**: Returns `void`
- **test0x0()**: Returns `void`
- **test2and0()**: Returns `void`

---

## LimitedInputStreamTest

**Description**: * Test suite for the {@link LimitedInputStream}.


### Public Methods:

- **testLimitation()**: Returns `void`

---

## ReflectionUtilsTest

**Description**: * Test suite for {@link ReflectionUtils}.

- **Extends**: MyParameterizedClassA
- **Implements**: MyParameterizedInterface

### Public Methods:

- **testEvaluateWithInstantiation()**: Returns `void`
- **testEvaluateWithStaticVariable()**: Returns `void`
- **testEvaluateWithStaticMethod()**: Returns `void`
- **testGetTypeParametersWithReusedTypeParameters()**: Returns `void`
- **testGetTypeParametersWithIndirectTypeParameters()**: Returns `void`

---

## WayangCollectionsTest

**Description**: * Test suite for {@link WayangCollections}.


### Public Methods:

- **testCreatePowerList()**: Returns `void`

---

## ExpressionBuilderTest

**Description**: * Test suite for the class {@link ExpressionBuilder}.


### Public Methods:

- **shouldNotFailOnValidInput()**: Returns `void`
- **shouldFailOnInvalidInput()**: Returns `void`

---

## ExpressionTest

**Description**: * Test suite for the {@link Expression} subclasses.


### Public Methods:

- **testSingletonExpressions()**: Returns `void`
- **testFailsOnMissingContext()**: Returns `void`
- **testComplexExpressions()**: Returns `void`
- **testSpecification()**: Returns `void`

---

## ProfileDB

**Description**: * This class provides facilities to save and load {@link Experiment}s.

- **Extends**: Measurement

### Fields:

- **storage**: `Storage` (private)
- **gson**: `Gson` (private)

### Public Methods:

- **save()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **append()**: Returns `void`
- **append()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **load()**: Returns `Collection<Experiment>`
- **getStorage()**: Returns `Storage`
- **registerMeasurementClass()**: Returns `ProfileDB`
- **withGsonPreparation()**: Returns `ProfileDB`
- **getGson()**: Returns `Gson`

---

## StopWatch

**Description**: * Utility to create {@link TimeMeasurement}s for an {@link Experiment}.


### Public Methods:

- **getOrCreateRound()**: Returns `TimeMeasurement`
- **start()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`
- **toPrettyString()**: Returns `String`
- **toPrettyString()**: Returns `String`
- **stopAll()**: Returns `void`
- **getExperiment()**: Returns `Experiment`

### Private Methods:

- **determineFirstColumnWidth()**: Returns `int`
- **determineFirstColumnWidth()**: Returns `int`
- **append()**: Returns `void`

---

## MeasurementDeserializer

**Description**: * Custom deserializer for {@link Measurement}s
 * Detects actual subclass of serialized instances and then delegates the deserialization to that subtype.

- **Extends**: Measurement
- **Implements**: JsonDeserializer

### Public Methods:

- **register()**: Returns `void`
- **deserialize()**: Returns `Measurement`

---

## MeasurementSerializer

**Description**: * Custom serializer for {@link Measurement}s
 * Detects actual subclass of given instances, encodes this class membership, and then delegates serialization to that subtype.

- **Implements**: JsonSerializer

### Public Methods:

- **serialize()**: Returns `JsonElement`

---

## Experiment

**Description**: * An experiment comprises {@link Measurement}s from one specific {@link Subject} execution.


### Fields:

- **id**: `String` (private)
- **description**: `String` (private)
- **startTime**: `long` (private)
- **tags**: `Collection<String>` (private)
- **measurements**: `Collection<Measurement>` (private)
- **subject**: `Subject` (private)

### Public Methods:

- **withDescription()**: Returns `Experiment`
- **getId()**: Returns `String`
- **setId()**: Returns `void`
- **getDescription()**: Returns `String`
- **setDescription()**: Returns `void`
- **getStartTime()**: Returns `long`
- **setStartTime()**: Returns `void`
- **getTags()**: Returns `Collection<String>`
- **setTags()**: Returns `void`
- **addMeasurement()**: Returns `void`
- **getMeasurements()**: Returns `Collection<Measurement>`
- **getSubject()**: Returns `Subject`
- **setSubject()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

---

## Measurement
- **Extends**: Measurement

### Fields:

- **id**: `String` (private)

### Public Methods:

- **getId()**: Returns `String`
- **setId()**: Returns `void`
- **getType()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

## Subject

**Description**: * The subject of an {@link Experiment}, e.g., an application or algorithm.


### Fields:

- **id**: `String` (private)
- **version**: `String` (private)

### Public Methods:

- **addConfiguration()**: Returns `Subject`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

## Type

---

## TimeMeasurement
- **Extends**: Measurement

### Public Methods:

- **start()**: Returns `void`
- **start()**: Returns `TimeMeasurement`
- **getOrCreateRound()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`
- **stop()**: Returns `void`
- **getMillis()**: Returns `long`
- **setMillis()**: Returns `void`
- **getRounds()**: Returns `Collection<TimeMeasurement>`
- **addRounds()**: Returns `void`
- **toString()**: Returns `String`

### Private Methods:

- **ensureStarted()**: Returns `void`
- **start()**: Returns `TimeMeasurement`
- **getRound()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`

---

## FileStorage
- **Extends**: Storage

### Fields:

- **file**: `File` (private)

### Public Methods:

- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **append()**: Returns `void`
- **append()**: Returns `void`

---

## JDBCStorage
- **Extends**: Storage

### Fields:

- **file**: `File` (private)

### Public Methods:

- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **append()**: Returns `void`
- **append()**: Returns `void`

---

## Storage

### Fields:

- **storageFile**: `URI` (private)
- **context**: `ProfileDB` (private)

### Public Methods:

- **setContext()**: Returns `void`
- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **load()**: Returns `Collection<Experiment>`

---

## ProfileDBTest

### Public Methods:

- **testPolymorphSaveAndLoad()**: Returns `void`
- **testRecursiveSaveAndLoad()**: Returns `void`
- **testFileOperations()**: Returns `void`
- **testAppendOnNonExistentFile()**: Returns `void`

---

## TestMemoryMeasurement
- **Extends**: Measurement

### Fields:

- **timestamp**: `long` (private)
- **usedMb**: `long` (private)

### Public Methods:

- **getTimestamp()**: Returns `long`
- **setTimestamp()**: Returns `void`
- **getUsedMb()**: Returns `long`
- **setUsedMb()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

---

## TestTimeMeasurement
- **Extends**: Measurement

### Fields:

- **millis**: `long` (private)
- **submeasurements**: `Collection<Measurement>` (private)

### Public Methods:

- **getMillis()**: Returns `long`
- **setMillis()**: Returns `void`
- **getSubmeasurements()**: Returns `Collection<Measurement>`
- **addSubmeasurements()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

# wayang-docs

# wayang-platforms

## Flink

**Description**: * Register for relevant components of this module.


---

## ChannelConversions

**Description**: * Register for the {@link ChannelConversion}s supported for this platform.


---

## DataSetChannel

**Description**: * Describes the situation where one {@link DataSet} is operated on, producing a further {@link DataSet}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>

- **Extends**: Channel

### Fields:

- **size**: `long` (private)

### Public Methods:

- **copy()**: Returns `Channel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **getChannel()**: Returns `DataSetChannel`

---

## FlinkCoGroupFunction

**Description**: * Wrapper of {@Link CoGroupFunction} of Flink for use in Wayang

- **Implements**: CoGroupFunction

### Public Methods:

- **coGroup()**: Returns `void`

---

## FunctionCompiler

**Description**: * A compiler translates Wayang functions into executable Java functions.

- **Implements**: PairFunction

### Public Methods:

- **getWayangFunction()**: Returns `Object`
- **call()**: Returns `Type`
- **getWayangFunction()**: Returns `Object`

---

## KeySelectorDistinct

**Description**: * Wrapper for {@Link KeySelector}

- **Implements**: KeySelector

### Public Methods:

- **getKey()**: Returns `String`

---

## KeySelectorFunction

**Description**: * Wrapper for {@Link KeySelector}

- **Implements**: KeySelector

### Fields:

- **key**: `Class<K>` (public)
- **typeInformation**: `TypeInformation<K>` (public)

### Public Methods:

- **getKey()**: Returns `K`
- **getProducedType()**: Returns `TypeInformation`

---

## OutputFormatConsumer

**Description**: * Wrapper for {@Link OutputFormat}

- **Implements**: OutputFormat

### Public Methods:

- **configure()**: Returns `void`
- **open()**: Returns `void`
- **writeRecord()**: Returns `void`
- **close()**: Returns `void`

---

## WayangFileOutputFormat

**Description**: * Wrapper for {@link FileOutputFormat}

- **Extends**: FileOutputFormat
- **Implements**: InitializeOnMaster, CleanupWhenUnsuccessful

### Fields:

- **blockPos**: `int` (private)
- **headerStream**: `DataOutputView` (private)

### Public Methods:

- **setOutputFilePath()**: Returns `void`
- **getOutputFilePath()**: Returns `Path`
- **setWriteMode()**: Returns `void`
- **setOutputDirectoryMode()**: Returns `void`
- **configure()**: Returns `void`
- **open()**: Returns `void`
- **writeRecord()**: Returns `void`
- **close()**: Returns `void`
- **initializeGlobal()**: Returns `void`
- **tryCleanupOnError()**: Returns `void`
- **close()**: Returns `void`
- **startRecord()**: Returns `void`
- **write()**: Returns `void`
- **write()**: Returns `void`
- **write()**: Returns `void`

### Private Methods:

- **writeInfo()**: Returns `void`

---

## DummyFilter

**Description**: * Create a {@Link FilterFunction} that remove the elements null

- **Implements**: FilterFunction

### Public Methods:

- **filter()**: Returns `boolean`
- **getProducedType()**: Returns `TypeInformation<InputType>`

---

## DummyMap

**Description**: * Class create a {@Link MapFunction} that genereta only null as convertion

- **Implements**: MapFunction

### Public Methods:

- **map()**: Returns `OutputType`
- **getProducedType()**: Returns `TypeInformation<OutputType>`

---

## WayangAggregator

**Description**: * Class create a {@Link Aggregator} that generate aggregatorWrapper

- **Implements**: Aggregator

### Fields:

- **elements**: `List<WayangValue>` (private)

### Public Methods:

- **getAggregate()**: Returns `ListValue<WayangValue>`
- **aggregate()**: Returns `void`
- **aggregate()**: Returns `void`
- **reset()**: Returns `void`

---

## WayangConvergenceCriterion

**Description**: * Class create a {@Link ConvergenceCriterion} that generate aggregatorWrapper

- **Implements**: ConvergenceCriterion

### Fields:

- **doWhile**: `boolean` (private)

### Public Methods:

- **setDoWhile()**: Returns `WayangConvergenceCriterion`
- **isConverged()**: Returns `boolean`

---

## WayangFilterCriterion

**Description**: * Class create a {@Link FilterFunction} for use inside of the LoopOperators

- **Extends**: AbstractRichFunction
- **Implements**: FilterFunction

### Fields:

- **wayangAggregator**: `WayangAggregator` (private)
- **name**: `String` (private)

### Public Methods:

- **open()**: Returns `void`
- **filter()**: Returns `boolean`

---

## WayangListValue

**Description**: * Is a Wrapper for used in the criterion of the Loops

- **Extends**: ListValue

---

## WayangValue

**Description**: * Implementation of {@link Value} of flink for use in Wayang

- **Implements**: Value

### Fields:

- **data**: `T` (private)

### Public Methods:

- **write()**: Returns `void`
- **read()**: Returns `void`
- **convertToObject()**: Returns `T`
- **toString()**: Returns `String`
- **get()**: Returns `T`

---

## FlinkContextReference

**Description**: * Wraps and manages a Flink {@link ExecutionEnvironment} to avoid steady re-creation.

- **Extends**: ExecutionResourceTemplate

### Fields:

- **flinkEnviroment**: `ExecutionEnvironment` (private)

### Public Methods:

- **get()**: Returns `ExecutionEnvironment`
- **isDisposed()**: Returns `boolean`

### Private Methods:

- **loadConfiguration()**: Returns `void`
- **getExecutionMode()**: Returns `ExecutionMode`

---

## FlinkExecutionContext

**Description**: * {@link ExecutionContext} implementation for the {@link FlinkPlatform}.

- **Implements**: ExecutionContext, Serializable

### Fields:

- **richFunction**: `RichFunction` (private)

### Public Methods:

- **setRichFunction()**: Returns `void`
- **getCurrentIteration()**: Returns `int`

---

## FlinkExecutor

**Description**: * {@link Executor} implementation for the {@link FlinkPlatform}.

- **Extends**: PushExecutorTemplate

### Fields:

- **flinkContextReference**: `FlinkContextReference` (private)
- **fee**: `ExecutionEnvironment` (public)
- **platform**: `FlinkPlatform` (private)
- **numDefaultPartitions**: `int` (private)

### Public Methods:

- **dispose()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **getCompiler()**: Returns `FunctionCompiler`
- **getNumDefaultPartitions()**: Returns `int`

---

## CartesianMapping

**Description**: * Mapping from {@link CartesianOperator} to {@link SparkCartesianOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## CoGroupMapping

**Description**: * Mapping from {@link CoGroupOperator} to {@link SparkCoGroupOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## CollectionSourceMapping

**Description**: * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## CountMapping

**Description**: * Mapping from {@link CountOperator} to {@link SparkCountOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## DistinctMapping

**Description**: * Mapping from {@link DistinctOperator} to {@link SparkDistinctOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## DoWhileMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## FilterMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## FlatMapMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## GlobalMaterializedGroupMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## GlobalReduceMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## GroupByMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## IntersectMapping

**Description**: * Mapping from {@link IntersectOperator} to {@link SparkIntersectOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## JoinMapping

**Description**: * Mapping from {@link JoinOperator} to {@link SparkJoinOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## LocalCallbackSinkMapping

**Description**: * Mapping from {@link LocalCallbackSink} to {@link SparkLocalCallbackSink}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## LoopMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## MapMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## MapPartitionsMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## Mappings

**Description**: * Register for the {@link Mapping}s supported for this platform.


---

## MaterializedGroupByMapping

**Description**: * Mapping from {@link MaterializedGroupByOperator} to {@link SparkMaterializedGroupByOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## ObjectFileSinkMapping

**Description**: * Mapping from {@link ObjectFileSink} to {@link SparkObjectFileSink}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## ObjectFileSourceMapping

**Description**: * Mapping from {@link ObjectFileSource} to {@link SparkObjectFileSource}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## PageRankMapping

**Description**: * Mapping from {@link PageRankOperator} to org.apache.wayang.spark.operators.graph.SparkPageRankOperator .

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## ReduceByMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## RepeatMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## SampleMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## SortMapping

**Description**: * Mapping from {@link SortOperator} to {@link SparkSortOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## TextFileSinkMapping

**Description**: * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## TextFileSourceMapping

**Description**: * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## UnionAllMapping

**Description**: * Mapping from {@link UnionAllOperator} to {@link SparkUnionAllOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## ZipWithIdMapping

**Description**: * Mapping from {@link ZipWithIdOperator} to {@link SparkZipWithIdOperator}.

- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## FlinkCartesianOperator

**Description**: * Flink implementation of the {@link CartesianOperator}.

- **Extends**: CartesianOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkCoGroupOperator

**Description**: * Flink implementation of the {@link CoGroupOperator}.

- **Extends**: CoGroupOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationTypeKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkCollectionSink

**Description**: * Converts {@link DataSetChannel} into a {@link CollectionChannel}

- **Extends**: UnaryToUnaryOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

---

## FlinkCollectionSource

**Description**: * This is execution operator implements the {@link CollectionSource}.

- **Extends**: CollectionSource
- **Implements**: the

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkCountOperator

**Description**: * Flink implementation of the {@link CountOperator}.

- **Extends**: CountOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkDistinctOperator

**Description**: * Flink implementation of the {@link DistinctOperator}.

- **Extends**: DistinctOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkDoWhileOperator

**Description**: * Flink implementation of the {@link DoWhileOperator}.

- **Extends**: DoWhileOperator
- **Implements**: FlinkExecutionOperator

### Fields:

- **iterativeDataSet**: `IterativeDataSet` (private)

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkExecutionOperator
- **Extends**: ExecutionOperator

---

## FlinkFilterOperator

**Description**: * Flink implementation of the {@link FilterOperator}.

- **Extends**: FilterOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkFlatMapOperator

**Description**: * Flink implementation of the {@link FlatMapOperator}.

- **Extends**: FlatMapOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkGlobalMaterializedGroupOperator

**Description**: * Flink implementation of the {@link GlobalMaterializedGroupOperator}.

- **Extends**: GlobalMaterializedGroupOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkGlobalReduceOperator

**Description**: * Flink implementation of the {@link GlobalReduceOperator}.

- **Extends**: GlobalReduceOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkGroupByOperator

**Description**: * Flink implementation of the {@link GroupByOperator}.

- **Extends**: GroupByOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkIntersectOperator

**Description**: * Flink implementation of the {@link IntersectOperator}.

- **Extends**: IntersectOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **map()**: Returns `Type`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkJoinOperator

**Description**: * Flink implementation of the {@link JoinOperator}.

- **Extends**: JoinOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkLocalCallbackSink

**Description**: * Implementation of the {@link LocalCallbackSink} operator for the Flink platform.

- **Extends**: Serializable
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkLoopOperator

**Description**: * Flink implementation of the {@link RepeatOperator}.

- **Extends**: LoopOperator
- **Implements**: FlinkExecutionOperator

### Fields:

- **iterativeDataSet**: `IterativeDataSet` (private)

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkMapOperator

**Description**: * Flink implementation of the {@link MapOperator}.

- **Extends**: MapOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkMapPartitionsOperator

**Description**: * Flink implementation of the {@link MapPartitionsOperator}.

- **Extends**: MapPartitionsOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkMaterializedGroupByOperator

**Description**: * Flink implementation of the {@link MaterializedGroupByOperator}.

- **Extends**: MaterializedGroupByOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkObjectFileSink

**Description**: * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
 *
 * @see FlinkObjectFileSink

- **Extends**: ObjectFileSink
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkObjectFileSource

**Description**: * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
 *
 * @see FlinkObjectFileSource

- **Extends**: ObjectFileSource
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **flatMap()**: Returns `void`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkPageRankOperator

**Description**: * Flink implementation of the {@link PageRankOperator}.

- **Extends**: PageRankOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **flatMap()**: Returns `void`
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **reduce()**: Returns `void`
- **flatMap()**: Returns `void`
- **filter()**: Returns `boolean`

---

## FlinkReduceByOperator

**Description**: * Flink implementation of the {@link ReduceByOperator}.

- **Extends**: ReduceByOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkRepeatExpandedOperator

**Description**: * Flink implementation of the {@link RepeatOperator}.

- **Extends**: RepeatOperator
- **Implements**: FlinkExecutionOperator

### Fields:

- **iterativeDataSet**: `IterativeDataSet<Type>` (private)

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkRepeatOperator

**Description**: * Flink implementation of the {@link RepeatOperator}.

- **Extends**: RepeatOperator
- **Implements**: FlinkExecutionOperator

### Fields:

- **iterationCounter**: `int` (private)
- **iterativeDataSet**: `IterativeDataSet<Type>` (private)

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## FlinkSampleOperator

**Description**: * Flink implementation of the {@link SampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)

- **Extends**: SampleOperator
- **Implements**: FlinkExecutionOperator

### Fields:

- **rand**: `Random` (private)

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **containsAction()**: Returns `boolean`

---

## FlinkSortOperator

**Description**: * Flink implementation of the {@link SortOperator}.

- **Extends**: SortOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkTextFileSink

**Description**: * Implementation of the {@link TextFileSink} operator for the Flink platform.

- **Extends**: TextFileSink
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

---

## FlinkTextFileSource

**Description**: * Provides a {@link Collection} to a Flink job.

- **Extends**: TextFileSource
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkTsvFileSink

**Description**: * Created by bertty on 31-10-17.

- **Extends**: Tuple2
- **Implements**: FlinkExecutionOperator

### Fields:

- **dataQuantum**: `Type` (private)

### Public Methods:

- **map()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkUnionAllOperator

**Description**: * Flink implementation of the {@link UnionAllOperator}.

- **Extends**: UnionAllOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkZipWithIdOperator

**Description**: * Flink implementation of the {@link MapOperator}.

- **Extends**: ZipWithIdOperator
- **Implements**: FlinkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## FlinkPlatform

**Description**: * {@link Platform} for Apache Flink.

- **Extends**: Platform

### Public Methods:

- **getFlinkContext()**: Returns `FlinkContextReference`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

---

## FlinkBasicPlugin

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link FlinkPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## FlinkConversionPlugin

**Description**: * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link FlinkPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## FlinkGraphPlugin

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link FlinkPlatform}.


---

## FlinkCartesianOperatorTest

**Description**: * Test suite for {@link FlinkCartesianOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkCoGroupOperatorTest

**Description**: * Test suite for {@link FlinkCoGroupOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

### Private Methods:

- **compare()**: Returns `boolean`

---

## FlinkCollectionSourceTest

**Description**: * Test suite for the {@link FlinkCollectionSource}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkCountOperatorTest

**Description**: * Test suite for {@link FlinkCountOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkDistinctOperatorTest
- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkFilterOperatorTest

**Description**: * Test suite for {@link FlinkFilterOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkFlatMapOperatorTest

**Description**: * Test suite for {@link FlinkFlatMapOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkGlobalMaterializedGroupOperatorTest

**Description**: * Test suite for {@link FlinkGlobalMaterializedGroupOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkGlobalReduceOperatorTest

**Description**: * Test suite for {@link FlinkGlobalReduceOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

---

## FlinkJoinOperatorTest

**Description**: * Test suite for {@link FlinkJoinOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkMapPartitionsOperatorTest

**Description**: * Test suite for {@link FlinkMapPartitionsOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkMaterializedGroupByOperatorTest

**Description**: * Test suite for {@link FlinkMaterializedGroupByOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkOperatorTestBase

**Description**: * Test base for {@link FlinkExecutionOperator} tests.


### Public Methods:

- **setUp()**: Returns `void`
- **getEnv()**: Returns `ExecutionEnvironment`

---

## FlinkReduceByOperatorTest

**Description**: * Test suite for {@link FlinkReduceByOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkSortOperatorTest

**Description**: * Test suite for {@link FlinkSortOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## FlinkUnionAllOperatorTest

**Description**: * Test suite for {@link FlinkUnionAllOperator}.

- **Extends**: FlinkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## ChannelFactory

**Description**: * Utility to create {@link Channel}s in tests.


### Public Methods:

- **setUp()**: Returns `void`

---

## Giraph

**Description**: * Register for relevant components of this module.


---

## PageRankAlgorithm

**Description**: * Basic PageRank implementation.

- **Extends**: BasicComputation

### Public Methods:

- **compute()**: Returns `void`
- **preApplication()**: Returns `void`
- **postApplication()**: Returns `void`
- **preSuperstep()**: Returns `void`
- **postSuperstep()**: Returns `void`
- **initialize()**: Returns `void`
- **nextVertex()**: Returns `boolean`
- **createVertexWriter()**: Returns `TextVertexWriter`
- **writeVertex()**: Returns `void`

---

## PageRankParameters

**Description**: * Parameters for Basic PageRank implementation.


---

## GiraphExecutor

**Description**: * {@link Executor} for the {@link GiraphPlatform}.

- **Extends**: ExecutorTemplate

### Fields:

- **configuration**: `Configuration` (private)
- **job**: `Job` (private)
- **giraphConfiguration**: `GiraphConfiguration` (private)

### Public Methods:

- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `GiraphPlatform`
- **getGiraphConfiguration()**: Returns `GiraphConfiguration`

### Private Methods:

- **execute()**: Returns `void`

---

## GiraphExecutionOperator
- **Extends**: ExecutionOperator

---

## GiraphPageRankOperator

**Description**: * PageRank {@link Operator} implementation for the {@link GiraphPlatform}.

- **Extends**: PageRankOperator
- **Implements**: GiraphExecutionOperator

### Fields:

- **path_out**: `String` (private)

### Public Methods:

- **getPlatform()**: Returns `Platform`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **setPathOut()**: Returns `void`
- **getPathOut()**: Returns `String`

---

## GiraphPlatform

**Description**: * Giraph {@link Platform} for Wayang.

- **Extends**: Platform

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

### Private Methods:

- **initialize()**: Returns `void`

---

## GiraphPlugin

**Description**: * This {@link Plugin} activates default capabilities of the {@link GiraphPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## GiraphPagaRankOperatorTest

**Description**: * Test For GiraphPageRank


### Public Methods:

- **setUp()**: Returns `void`
- **testExecution()**: Returns `void`

---

## GraphChi

**Description**: * Register for relevant components of this module.


---

## GraphChiExecutor

**Description**: * {@link Executor} for the {@link GraphChiPlatform}.

- **Extends**: ExecutorTemplate

### Public Methods:

- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `GraphChiPlatform`

### Private Methods:

- **execute()**: Returns `void`

---

## GraphChiExecutionOperator
- **Extends**: ExecutionOperator

---

## GraphChiPageRankOperator

**Description**: * PageRank {@link Operator} implementation for the {@link GraphChiPlatform}.

- **Extends**: PageRankOperator
- **Implements**: GraphChiExecutionOperator

### Public Methods:

- **getPlatform()**: Returns `Platform`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## GraphChiPlatform

**Description**: * GraphChi {@link Platform} for Wayang.

- **Extends**: Platform

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

### Private Methods:

- **initialize()**: Returns `void`

---

## GraphChiPlugin

**Description**: * This {@link Plugin} activates default capabilities of the {@link GraphChiPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## GraphChiPageRankOperatorTest

**Description**: * Test suite for the {@link GraphChiPageRankOperator}.


### Public Methods:

- **setUp()**: Returns `void`
- **testExecution()**: Returns `void`

---

## Java

**Description**: * Register for relevant components of this module.


---

## CollectionChannel

**Description**: * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.

- **Extends**: Channel
- **Implements**: JavaChannelInstance

### Public Methods:

- **copy()**: Returns `CollectionChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getChannel()**: Returns `Channel`

---

## JavaChannelInstance
- **Extends**: ChannelInstance

---

## StreamChannel

**Description**: * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.

- **Extends**: Channel
- **Implements**: JavaChannelInitializer

### Public Methods:

- **copy()**: Returns `StreamChannel`
- **exchangeWith()**: Returns `Channel`
- **createInstance()**: Returns `Instance`
- **setUpOutput()**: Returns `Channel`
- **provideStreamChannel()**: Returns `StreamChannel`
- **accept()**: Returns `void`
- **getChannel()**: Returns `Channel`
- **getMeasuredCardinality()**: Returns `OptionalLong`

---

## JavaExecutionContext

**Description**: * {@link ExecutionContext} implementation for the {@link JavaPlatform}.

- **Implements**: ExecutionContext

### Public Methods:

- **getCurrentIteration()**: Returns `int`

---

## JavaExecutor

**Description**: * {@link Executor} implementation for the {@link JavaPlatform}.

- **Extends**: PushExecutorTemplate

### Public Methods:

- **getPlatform()**: Returns `JavaPlatform`
- **getCompiler()**: Returns `FunctionCompiler`

---

## JavaCartesianOperator

**Description**: * Java implementation of the {@link CartesianOperator}.

- **Extends**: CartesianOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaCoGroupOperator

**Description**: * Java implementation of the {@link CoGroupOperator}.

- **Extends**: CoGroupOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaCollectOperator

**Description**: * Converts {@link StreamChannel} into a {@link CollectionChannel}

- **Extends**: UnaryToUnaryOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

---

## JavaCollectionSource

**Description**: * This is execution operator implements the {@link TextFileSource}.

- **Extends**: CollectionSource
- **Implements**: the

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaCountOperator

**Description**: * Java implementation of the {@link CountOperator}.

- **Extends**: CountOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaDistinctOperator

**Description**: * Java implementation of the {@link DistinctOperator}.

- **Extends**: DistinctOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaDoWhileOperator

**Description**: * Java implementation of the {@link DoWhileOperator}.

- **Extends**: DoWhileOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaExecutionOperator
- **Extends**: ExecutionOperator

---

## JavaFilterOperator

**Description**: * Java implementation of the {@link FilterOperator}.

- **Extends**: FilterOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaFlatMapOperator

**Description**: * Java implementation of the {@link FlatMapOperator}.

- **Extends**: FlatMapOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaGlobalMaterializedGroupOperator

**Description**: * Java implementation of the {@link GlobalMaterializedGroupOperator}.

- **Extends**: GlobalMaterializedGroupOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

---

## JavaGlobalReduceOperator

**Description**: * Java implementation of the {@link GlobalReduceOperator}.

- **Extends**: GlobalReduceOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaIntersectOperator

**Description**: * Java implementation of the {@link IntersectOperator}.

- **Extends**: IntersectOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Private Methods:

- **createProbingTable()**: Returns `Set<Type>`

---

## JavaJoinOperator

**Description**: * Java implementation of the {@link JoinOperator}.

- **Extends**: JoinOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaLocalCallbackSink

**Description**: * Implementation of the {@link LocalCallbackSink} operator for the Java platform.

- **Extends**: Serializable
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaLoopOperator

**Description**: * Java implementation of the {@link LoopOperator}.

- **Extends**: LoopOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaMapOperator

**Description**: * Java implementation of the {@link org.apache.wayang.basic.operators.MapOperator}.

- **Extends**: MapOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaMapPartitionsOperator

**Description**: * Java implementation of the {@link MapPartitionsOperator}.

- **Extends**: MapPartitionsOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaMaterializedGroupByOperator

**Description**: * Java implementation of the {@link MaterializedGroupByOperator}.

- **Extends**: MaterializedGroupByOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaObjectFileSink

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSource

- **Extends**: ObjectFileSink
- **Implements**: JavaExecutionOperator

### Fields:

- **nextIndex**: `int` (private)

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **push()**: Returns `void`
- **fire()**: Returns `void`

---

## JavaObjectFileSource

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink

- **Extends**: ObjectFileSource
- **Implements**: JavaExecutionOperator

### Fields:

- **nextElements_cole**: `ArrayList` (private)
- **nextIndex**: `int` (private)

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `T`
- **close()**: Returns `void`

### Private Methods:

- **tryAdvance()**: Returns `void`

---

## JavaRandomSampleOperator

**Description**: * Java implementation of the {@link JavaRandomSampleOperator}. This sampling method is with replacement (i.e., duplicates may appear in the sample).

- **Extends**: SampleOperator
- **Implements**: JavaExecutionOperator

### Fields:

- **rand**: `Random` (private)

### Public Methods:

- **test()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaReduceByOperator

**Description**: * Java implementation of the {@link ReduceByOperator}.

- **Extends**: ReduceByOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **supplier()**: Returns `Supplier<List<T>>`
- **combiner()**: Returns `BinaryOperator<List<T>>`
- **characteristics()**: Returns `Set<Characteristics>`

---

## JavaRepeatOperator

**Description**: * Java implementation of the {@link DoWhileOperator}.

- **Extends**: RepeatOperator
- **Implements**: JavaExecutionOperator

### Fields:

- **iterationCounter**: `int` (private)

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaReservoirSampleOperator

**Description**: * Java implementation of the {@link JavaReservoirSampleOperator}.

- **Extends**: SampleOperator
- **Implements**: JavaExecutionOperator

### Fields:

- **rand**: `Random` (private)

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaSortOperator

**Description**: * Java implementation of the {@link SortOperator}.

- **Extends**: SortOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaTextFileSink

**Description**: * Implementation fo the {@link TextFileSink} for the {@link JavaPlatform}.

- **Extends**: TextFileSink
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaTextFileSource

**Description**: * This is execution operator implements the {@link TextFileSource}.

- **Extends**: TextFileSource
- **Implements**: the

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **copy()**: Returns `JavaTextFileSource`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaTsvFileSink

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see JavaObjectFileSource

- **Extends**: Tuple2
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaTsvFileSource

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink

- **Extends**: UnarySource
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Private Methods:

- **createStream()**: Returns `Stream<T>`
- **lineParse()**: Returns `T`

---

## JavaUnionAllOperator

**Description**: * Java implementation of the {@link UnionAllOperator}.

- **Extends**: UnionAllOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaPageRankOperator

**Description**: * Java implementation of the {@link PageRankOperator}.

- **Extends**: PageRankOperator
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaPlatform

**Description**: * {@link Platform} for a single JVM executor based on the {@link java.util.stream.Stream} library.

- **Extends**: Platform

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

---

## JavaBasicPlugin

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## JavaChannelConversionPlugin

**Description**: * This {@link Plugin} is a subset of the {@link JavaBasicPlugin} and only ships with the {@link ChannelConversion}s.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## JavaGraphPlugin

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## JavaExecutorTest

**Description**: * Test suite for the {@link JavaExecutor}.


### Fields:

- **increment**: `int` (private)

### Public Methods:

- **testLazyExecutionResourceHandling()**: Returns `void`
- **apply()**: Returns `Integer`
- **open()**: Returns `void`

---

## JavaCartesianOperatorTest

**Description**: * Test suite for {@link JavaCartesianOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaCoGroupOperatorTest

**Description**: * Test suite for {@link JavaJoinOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

### Private Methods:

- **compare()**: Returns `boolean`

---

## JavaCollectionSourceTest

**Description**: * Test suite for the {@link JavaCollectionSource}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaCountOperatorTest

**Description**: * Test suite for {@link JavaCountOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaDistinctOperatorTest

**Description**: * Test suite for {@link JavaDistinctOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaExecutionOperatorTestBase

**Description**: * Superclass for tests of {@link JavaExecutionOperator}s.


---

## JavaFilterOperatorTest

**Description**: * Test suite for {@link JavaFilterOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaGlobalMaterializedGroupOperatorTest

**Description**: * Test suite for {@link JavaGlobalReduceOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

---

## JavaGlobalReduceOperatorTest

**Description**: * Test suite for {@link JavaGlobalReduceOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

---

## JavaJoinOperatorTest

**Description**: * Test suite for {@link JavaJoinOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaLocalCallbackSinkTest

**Description**: * Test suite for {@link JavaLocalCallbackSink}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaMaterializedGroupByOperatorTest

**Description**: * Test suite for {@link JavaReduceByOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaObjectFileSinkTest

**Description**: * Test suite for {@link JavaObjectFileSink}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

---

## JavaObjectFileSourceTest

**Description**: * Test suite for {@link JavaObjectFileSource}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testReading()**: Returns `void`

---

## JavaRandomSampleOperatorTest

**Description**: * Test suite for {@link JavaRandomSampleOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testUDFExecution()**: Returns `void`
- **testLargerSampleExecution()**: Returns `void`

---

## JavaReduceByOperatorTest

**Description**: * Test suite for {@link JavaReduceByOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaReservoirSampleOperatorTest

**Description**: * Test suite for {@link JavaReservoirSampleOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testLargerSampleExecution()**: Returns `void`

---

## JavaSortOperatorTest

**Description**: * Test suite for {@link JavaSortOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## JavaTextFileSinkTest

**Description**: * Test suite for {@link JavaTextFileSink}.

- **Extends**: JavaExecutionOperatorTestBase

### Fields:

- **defaultLocale**: `Locale` (private)

### Public Methods:

- **setupTest()**: Returns `void`
- **teardownTest()**: Returns `void`
- **testWritingLocalFile()**: Returns `void`

---

## JavaUnionAllOperatorTest

**Description**: * Test suite for {@link JavaUnionAllOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SqlQueryChannel

**Description**: * Implementation of a {@link Channel} that is given by a SQL query.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `SqlQueryChannel`
- **getChannel()**: Returns `SqlQueryChannel`
- **setSqlQuery()**: Returns `void`
- **getSqlQuery()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

## DatabaseDescriptor

**Description**: * This class describes a database.


### Public Methods:

- **createJdbcConnection()**: Returns `Connection`

---

## JdbcExecutor

**Description**: * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.

- **Extends**: ExecutorTemplate

### Public Methods:

- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `Platform`

### Private Methods:

- **findJdbcExecutionOperatorTaskInStage()**: Returns `ExecutionTask`
- **getSqlClause()**: Returns `String`
- **saveResult()**: Returns `void`

---

## JdbcExecutionOperator
- **Extends**: ExecutionOperator

---

## JdbcFilterOperator
- **Extends**: FilterOperator
- **Implements**: JdbcExecutionOperator

### Public Methods:

- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

---

## JdbcProjectionOperator
- **Extends**: MapOperator
- **Implements**: JdbcExecutionOperator

### Public Methods:

- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

---

## JdbcTableSource
- **Extends**: TableSource
- **Implements**: JdbcExecutionOperator

### Public Methods:

- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getCardinalityEstimator()**: Returns `CardinalityEstimator`
- **estimate()**: Returns `CardinalityEstimate`

---

## SqlToStreamOperator

**Description**: * This {@link Operator} converts {@link SqlQueryChannel}s to {@link StreamChannel}s.

- **Extends**: UnaryToUnaryOperator
- **Implements**: JavaExecutionOperator, JsonSerializable

### Fields:

- **resultSet**: `ResultSet` (private)
- **next**: `Record` (private)

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `Record`
- **close()**: Returns `void`
- **toJson()**: Returns `WayangJsonObj`

### Private Methods:

- **moveToNext()**: Returns `void`

---

## JdbcPlatformTemplate
- **Extends**: Platform

### Public Methods:

- **getConnection()**: Returns `Connection`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **getPlatformId()**: Returns `String`
- **createDatabaseDescriptor()**: Returns `DatabaseDescriptor`

### Private Methods:

- **getDefaultConfigurationFile()**: Returns `String`

---

## JdbcExecutorTest

**Description**: * Test suite for {@link JdbcExecutor}.


### Public Methods:

- **testExecuteWithPlainTableSource()**: Returns `void`
- **testExecuteWithFilter()**: Returns `void`
- **testExecuteWithProjection()**: Returns `void`
- **testExecuteWithProjectionAndFilters()**: Returns `void`

---

## JdbcTableSourceTest

**Description**: * Test suite for {@link SqlToStreamOperator}.


### Public Methods:

- **testCardinalityEstimator()**: Returns `void`

---

## OperatorTestBase

**Description**: * Test base for {@link JdbcExecutionOperator}s and other {@link ExecutionOperator}s in this module.


---

## SqlToStreamOperatorTest

**Description**: * Test suite for {@link SqlToStreamOperator}.

- **Extends**: OperatorTestBase

### Public Methods:

- **testWithHsqldb()**: Returns `void`
- **testWithEmptyHsqldb()**: Returns `void`

---

## HsqldbFilterOperator

**Description**: * Test implementation of {@link JdbcFilterOperator}.

- **Extends**: JdbcFilterOperator

### Public Methods:

- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## HsqldbPlatform

**Description**: * {@link JdbcPlatformTemplate} implementation based on HSQLDB for test purposes.

- **Extends**: JdbcPlatformTemplate

---

## HsqldbProjectionOperator

**Description**: * Test implementation of {@link JdbcFilterOperator}.

- **Extends**: JdbcProjectionOperator

### Public Methods:

- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## HsqldbTableSource

**Description**: * Test implementation of {@link JdbcFilterOperator}.

- **Extends**: JdbcTableSource

### Public Methods:

- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## Postgres

**Description**: * Register for relevant components of this module.


---

## ProjectionMapping
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

---

## PostgresExecutionOperator
- **Extends**: JdbcExecutionOperator

---

## PostgresFilterOperator

**Description**: * PostgreSQL implementation of the {@link FilterOperator}.

- **Extends**: JdbcFilterOperator
- **Implements**: PostgresExecutionOperator

---

## PostgresProjectionOperator

**Description**: * PostgreSQL implementation of the {@link FilterOperator}.

- **Extends**: JdbcProjectionOperator
- **Implements**: PostgresExecutionOperator

---

## PostgresTableSource

**Description**: * PostgreSQL implementation for the {@link TableSource}.

- **Extends**: JdbcTableSource
- **Implements**: PostgresExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`

---

## PostgresPlatform

**Description**: * {@link Platform} implementation for SQLite3.

- **Extends**: JdbcPlatformTemplate

### Public Methods:

- **getJdbcDriverClassName()**: Returns `String`

---

## PostgresConversionsPlugin

**Description**: * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link PostgresPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## PostgresPlugin

**Description**: * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link PostgresPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## Spark

**Description**: * Register for relevant components of this module.


---

## BroadcastChannel

**Description**: * {@link Channel} that represents a broadcasted value.

- **Extends**: Channel

### Public Methods:

- **copy()**: Returns `BroadcastChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getChannel()**: Returns `BroadcastChannel`

---

## FileChannels

**Description**: * Utilities for {@link FileChannel}s.


---

## RddChannel

**Description**: * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>

- **Extends**: Channel

### Fields:

- **accumulator**: `LongAccumulator` (private)

### Public Methods:

- **copy()**: Returns `RddChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **getChannel()**: Returns `RddChannel`

### Private Methods:

- **isRddCached()**: Returns `boolean`

---

## BinaryOperatorAdapter

**Description**: * Wraps a {@link java.util.function.BinaryOperator} as a {@link Function2}.

- **Implements**: Function2

### Fields:

- **binaryOperator**: `BinaryOperator<Type>` (private)

### Public Methods:

- **call()**: Returns `Type`

---

## ExtendedBinaryOperatorAdapter

**Description**: * Implements a {@link Function2} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function2#call(Object, Object)}.

- **Implements**: Function2

### Public Methods:

- **call()**: Returns `Type`

---

## ExtendedFlatMapFunctionAdapter

**Description**: * Implements a {@link FlatMapFunction} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.

- **Implements**: FlatMapFunction

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

---

## ExtendedFunction

**Description**: * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.

- **Extends**: Function
- **Implements**: Function

### Public Methods:

- **call()**: Returns `OutputType`

---

## ExtendedMapFunctionAdapter

**Description**: * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.

- **Implements**: Function

### Public Methods:

- **call()**: Returns `OutputType`

---

## ExtendedMapPartitionsFunctionAdapter

**Description**: * Wraps a {@link Function} as a {@link FlatMapFunction}.

- **Implements**: FlatMapFunction

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

---

## ExtendedPredicateAdapater

**Description**: * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.

- **Implements**: Function

### Public Methods:

- **call()**: Returns `Boolean`

---

## FlatMapFunctionAdapter

**Description**: * Wraps a {@link java.util.function.Function} as a {@link FlatMapFunction}.

- **Implements**: FlatMapFunction

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

---

## MapFunctionAdapter

**Description**: * Wraps a {@link java.util.function.Function} as a {@link Function}.

- **Implements**: Function

### Public Methods:

- **call()**: Returns `OutputType`

---

## MapPartitionsFunctionAdapter

**Description**: * Wraps a {@link Function} as a {@link FlatMapFunction}.

- **Implements**: FlatMapFunction

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

---

## PredicateAdapter

**Description**: * Wraps a {@link Predicate} as a {@link Function}.

- **Implements**: Function

### Fields:

- **predicate**: `Predicate<InputType>` (private)

### Public Methods:

- **call()**: Returns `Boolean`

---

## SparkContextReference

**Description**: * Wraps and manages a {@link JavaSparkContext} to avoid steady re-creation.

- **Extends**: ExecutionResourceTemplate

### Public Methods:

- **get()**: Returns `JavaSparkContext`

---

## SparkExecutionContext

**Description**: * {@link ExecutionContext} implementation for the {@link SparkPlatform}.

- **Implements**: ExecutionContext, Serializable

### Fields:

- **iterationNumber**: `int` (private)

### Public Methods:

- **getCurrentIteration()**: Returns `int`

---

## SparkExecutor

**Description**: * {@link Executor} implementation for the {@link SparkPlatform}.

- **Extends**: PushExecutorTemplate

### Public Methods:

- **forward()**: Returns `void`
- **getPlatform()**: Returns `SparkPlatform`
- **getNumDefaultPartitions()**: Returns `int`
- **dispose()**: Returns `void`
- **getCompiler()**: Returns `FunctionCompiler`

---

## SparkBernoulliSampleOperator

**Description**: * Spark implementation of the {@link SparkBernoulliSampleOperator}.

- **Extends**: SampleOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkBroadcastOperator

**Description**: * Takes care of creating a {@link Broadcast} that can be used later on.

- **Extends**: UnaryToUnaryOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getType()**: Returns `DataSetType<Type>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## SparkCacheOperator

**Description**: * Converts an uncached {@link RddChannel} into a cached {@link RddChannel}.

- **Extends**: UnaryToUnaryOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

---

## SparkCartesianOperator

**Description**: * Spark implementation of the {@link CartesianOperator}.

- **Extends**: CartesianOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkCoGroupOperator

**Description**: * Spark implementation of the {@link JoinOperator}.

- **Extends**: CoGroupOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkCollectOperator

**Description**: * Converts a {@link RddChannel} into a {@link CollectionChannel} of the {@link JavaPlatform}.

- **Extends**: UnaryToUnaryOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

---

## SparkCollectionSource

**Description**: * Provides a {@link Collection} to a Spark job. Can also be used to convert {@link CollectionChannel}s of the
 * {@link JavaPlatform} into {@link RddChannel}s.

- **Extends**: CollectionSource
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkCountOperator

**Description**: * Spark implementation of the {@link CountOperator}.

- **Extends**: CountOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkDistinctOperator

**Description**: * Spark implementation of the {@link DistinctOperator}.

- **Extends**: DistinctOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkDoWhileOperator

**Description**: * Spark implementation of the {@link DoWhileOperator}.

- **Extends**: DoWhileOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkExecutionOperator
- **Extends**: ExecutionOperator

---

## SparkFilterOperator

**Description**: * Spark implementation of the {@link FilterOperator}.

- **Extends**: FilterOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkFlatMapOperator

**Description**: * Spark implementation of the {@link FlatMapOperator}.

- **Extends**: FlatMapOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkGlobalMaterializedGroupOperator

**Description**: * Spark implementation of the {@link GlobalMaterializedGroupOperator}.

- **Extends**: GlobalMaterializedGroupOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkGlobalReduceOperator

**Description**: * Spark implementation of the {@link GlobalReduceOperator}.

- **Extends**: GlobalReduceOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkIntersectOperator

**Description**: * Spark implementation of the {@link JoinOperator}.

- **Extends**: IntersectOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkJoinOperator

**Description**: * Spark implementation of the {@link JoinOperator}.

- **Extends**: JoinOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkLocalCallbackSink

**Description**: * Implementation of the {@link LocalCallbackSink} operator for the Spark platform.

- **Extends**: Serializable
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkLoopOperator

**Description**: * Spark implementation of the {@link LoopOperator}.

- **Extends**: LoopOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkMapOperator

**Description**: * Spark implementation of the {@link org.apache.wayang.basic.operators.MapOperator}.

- **Extends**: MapOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkMapPartitionsOperator

**Description**: * Spark implementation of the {@link MapPartitionsOperator}.

- **Extends**: MapPartitionsOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkMaterializedGroupByOperator

**Description**: * Spark implementation of the {@link MaterializedGroupByOperator}.

- **Extends**: MaterializedGroupByOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **call()**: Returns `Iterable<Type>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkObjectFileSink

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSource

- **Extends**: ObjectFileSink
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkObjectFileSource

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink

- **Extends**: ObjectFileSource
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkRandomPartitionSampleOperator

**Description**: * Spark implementation of the {@link SampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)

- **Extends**: SampleOperator
- **Implements**: SparkExecutionOperator

### Fields:

- **rand**: `Random` (private)
- **start_id**: `int` (private)
- **end_id**: `int` (private)
- **ids**: `ArrayList<Integer>` (private)

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **containsAction()**: Returns `boolean`
- **apply()**: Returns `List<V>`
- **apply()**: Returns `List<V>`

---

## SparkReduceByOperator

**Description**: * Spark implementation of the {@link ReduceByOperator}.

- **Extends**: ReduceByOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **call()**: Returns `InputType`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkRepeatOperator

**Description**: * Spark implementation of the {@link RepeatOperator}.

- **Extends**: RepeatOperator
- **Implements**: SparkExecutionOperator

### Fields:

- **iterationCounter**: `int` (private)

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkShufflePartitionSampleOperator

**Description**: * Spark implementation of the {@link SparkShufflePartitionSampleOperator}.

- **Extends**: SampleOperator
- **Implements**: SparkExecutionOperator

### Fields:

- **rand**: `Random` (private)
- **partitions**: `List<Integer>` (private)
- **shuffledRDD**: `JavaRDD<Type>` (private)
- **partitionID**: `int` (private)
- **rand**: `Random` (private)
- **start_id**: `int` (private)
- **end_id**: `int` (private)

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **call()**: Returns `Object`
- **apply()**: Returns `List<V>`

---

## SparkSortOperator

**Description**: * Spark implementation of the {@link SortOperator}.

- **Extends**: SortOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkTextFileSink

**Description**: * Implementation of the {@link TextFileSink} operator for the Spark platform.

- **Extends**: TextFileSink
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

---

## SparkTextFileSource

**Description**: * Provides a {@link Collection} to a Spark job.

- **Extends**: TextFileSource
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkTsvFileSink

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see SparkObjectFileSource

- **Extends**: Tuple2
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkTsvFileSource

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see SparkObjectFileSink

- **Extends**: UnarySource
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkUnionAllOperator

**Description**: * Spark implementation of the {@link UnionAllOperator}.

- **Extends**: UnionAllOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkZipWithIdOperator

**Description**: * Spark implementation of the {@link MapOperator}.

- **Extends**: ZipWithIdOperator
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkPlatform

**Description**: * {@link Platform} for Apache Spark.

- **Extends**: Platform

### Fields:

- **sparkContextReference**: `SparkContextReference` (private)

### Public Methods:

- **getSparkContext()**: Returns `SparkContextReference`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **warmUp()**: Returns `void`
- **getInitializeMillis()**: Returns `long`

### Private Methods:

- **registerJarIfNotNull()**: Returns `void`

---

## SparkBasicPlugin

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## SparkConversionPlugin

**Description**: * This {@link Plugin} enables to create {@link org.apache.spark.rdd.RDD}s.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## SparkGraphPlugin

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.

- **Implements**: Plugin

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

---

## SparkBernoulliSampleOperatorTest

**Description**: * Test suite for {@link SparkBernoulliSampleOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testDoesNotFail()**: Returns `void`

---

## SparkCartesianOperatorTest

**Description**: * Test suite for {@link SparkCartesianOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkCoGroupOperatorTest

**Description**: * Test suite for {@link SparkJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

### Private Methods:

- **compare()**: Returns `boolean`

---

## SparkCollectionSourceTest

**Description**: * Test suite for the {@link SparkCollectionSource}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkCountOperatorTest

**Description**: * Test suite for {@link SparkCountOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkDistinctOperatorTest

**Description**: * Test suite for {@link SparkDistinctOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkFilterOperatorTest

**Description**: * Test suite for {@link SparkFilterOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkFlatMapOperatorTest

**Description**: * Test suite for {@link SparkFilterOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkGlobalMaterializedGroupOperatorTest

**Description**: * Test suite for {@link SparkGlobalMaterializedGroupOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

---

## SparkGlobalReduceOperatorTest

**Description**: * Test suite for {@link SparkGlobalReduceOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

---

## SparkJoinOperatorTest

**Description**: * Test suite for {@link SparkJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkMapPartitionsOperatorTest

**Description**: * Test suite for {@link SparkFilterOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkMaterializedGroupByOperatorTest

**Description**: * Test suite for {@link SparkMaterializedGroupByOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkObjectFileSinkTest

**Description**: * Test suite for {@link SparkObjectFileSink}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

---

## SparkObjectFileSourceTest

**Description**: * Test suite for {@link SparkObjectFileSource}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

---

## SparkOperatorTestBase

**Description**: * Test base for {@link SparkExecutionOperator} tests.


### Public Methods:

- **setUp()**: Returns `void`
- **getSC()**: Returns `JavaSparkContext`

---

## SparkRandomPartitionSampleOperatorTest

**Description**: * Test suite for {@link SparkRandomPartitionSampleOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testUDFExecution()**: Returns `void`

---

## SparkReduceByOperatorTest

**Description**: * Test suite for {@link SparkReduceByOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkShufflePartitionSampleOperatorTest

**Description**: * Test suite for {@link SparkShufflePartitionSampleOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithUnknownDatasetSize()**: Returns `void`

---

## SparkSortOperatorTest

**Description**: * Test suite for {@link SparkSortOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkTextFileSinkTest

**Description**: * Test suite for {@link SparkTextFileSink}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

---

## SparkUnionAllOperatorTest

**Description**: * Test suite for {@link SparkUnionAllOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## Sqlite3

**Description**: * Register for relevant components of this module.


---

## Sqlite3FilterOperator

**Description**: * Implementation of the {@link FilterOperator} for the {@link Sqlite3Platform}.

- **Extends**: JdbcFilterOperator

### Public Methods:

- **getPlatform()**: Returns `Sqlite3Platform`

---

## Sqlite3ProjectionOperator

**Description**: * Implementation of the {@link JdbcProjectionOperator} for the {@link Sqlite3Platform}.

- **Extends**: JdbcProjectionOperator

### Public Methods:

- **getPlatform()**: Returns `Sqlite3Platform`

---

## Sqlite3TableSource

**Description**: * Implementation of the {@link TableSource} for the {@link Sqlite3Platform}.

- **Extends**: JdbcTableSource

### Public Methods:

- **getPlatform()**: Returns `Sqlite3Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`

---

## Sqlite3Platform

**Description**: * {@link Platform} implementation for SQLite3.

- **Extends**: JdbcPlatformTemplate

### Public Methods:

- **getJdbcDriverClassName()**: Returns `String`

---

## Sqlite3ConversionPlugin

**Description**: * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link Sqlite3Platform}.

- **Implements**: Plugin

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## Sqlite3Plugin

**Description**: * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link Sqlite3Platform}.

- **Implements**: Plugin

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

# wayang-plugins

## IEJoin

**Description**: * Provides {@link Plugin}s that enable usage of the {@link IEJoinOperator} and the {@link IESelfJoinOperator}.


### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

## Data

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable
- **Implements**: Serializable, Comparable

### Public Methods:

- **resetPivot()**: Returns `void`
- **isPivot()**: Returns `boolean`
- **getRowID()**: Returns `long`
- **setRowID()**: Returns `void`
- **getValue()**: Returns `Type0`
- **getRank()**: Returns `Type1`
- **compareTo()**: Returns `int`
- **compareRank()**: Returns `int`
- **compareTo()**: Returns `int`
- **toString()**: Returns `String`
- **compare()**: Returns `int`

---

## Mappings

**Description**: * {@link Mapping}s for the {@link IEJoinOperator}.


---

## IEJoinMapping

**Description**: * Mapping from {@link IEJoinOperator} to {@link SparkIEJoinOperator}.

- **Extends**: Record
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`

---

## IESelfJoinMapping

**Description**: * Mapping from {@link IESelfJoinOperator} to {@link SparkIESelfJoinOperator}.

- **Extends**: Record
- **Implements**: Mapping

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`

### Private Methods:

- **createSubplanPattern()**: Returns `SubplanPattern`

---

## IEJoinMasterOperator

**Description**: * This operator decides the correct sorting orders for IEJoin


---

## IEJoinOperator

**Description**: * This operator applies inequality join on elements of input datasets.

- **Extends**: BinaryToUnaryOperator

### Public Methods:

- **assignSortOrders()**: Returns `void`

---

## IESelfJoinOperator

**Description**: * This operator applies inequality self join on elements of input datasets.

- **Extends**: Comparable

### Public Methods:

- **assignSortOrders()**: Returns `void`

---

## JavaIEJoinOperator

**Description**: * Java implementation of the {@link IEJoinOperator}.

- **Extends**: Comparable
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## JavaIESelfJoinOperator

**Description**: * Java implementation of the {@link IESelfJoinOperator}.

- **Extends**: Comparable
- **Implements**: JavaExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

---

## SparkIEJoinOperator

**Description**: * Spark implementation of the {@link   IEJoinOperator}.

- **Extends**: Comparable
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## SparkIESelfJoinOperator

**Description**: * Spark implementation of the {@link   IESelfJoinOperator}.

- **Extends**: Comparable
- **Implements**: SparkExecutionOperator

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

---

## BitSetJoin

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable

---

## DataComparator

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable
- **Implements**: Serializable, Comparator

### Public Methods:

- **compare()**: Returns `int`

---

## extractData

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable
- **Implements**: Function

### Public Methods:

- **call()**: Returns `Data`

---

## myMergeSort

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable

---

## revDataComparator
- **Implements**: Serializable, Comparator

### Public Methods:

- **compare()**: Returns `int`

---

## List2AttributesObjectSkinny

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable
- **Implements**: Serializable

### Public Methods:

- **getHeadTupleValue()**: Returns `Type0`
- **getPartitionID()**: Returns `long`
- **isEmpty()**: Returns `boolean`
- **toString()**: Returns `String`

---

## addUniqueID

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Copyable

---

## build2ListObject

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable

---

## filterUnwantedBlocks

**Description**: * Created by khayyzy on 5/28/16.

- **Extends**: Comparable

### Public Methods:

- **call()**: Returns `Boolean`

### Private Methods:

- **compareMinMax()**: Returns `boolean`
- **compare()**: Returns `boolean`

---

## JavaExecutionOperatorTestBase

**Description**: * Superclass for tests of {@link JavaExecutionOperator}s.


---

## JavaIEJoinOperatorTest

**Description**: * Test suite for {@link JavaIEJoinOperator}.

- **Extends**: JavaExecutionOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkIEJoinOperatorTest

**Description**: * Test suite for {@link SparkIEJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkIEJoinOperatorTest2

**Description**: * Test suite for {@link SparkIEJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkIEJoinOperatorTest3

**Description**: * Test suite for {@link SparkIEJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkIEJoinOperatorTest4

**Description**: * Test suite for {@link SparkIEJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkIESelfJoinOperatorTest

**Description**: * Test suite for {@link SparkIEJoinOperator}.

- **Extends**: SparkOperatorTestBase

### Public Methods:

- **testExecution()**: Returns `void`

---

## SparkOperatorTestBase

**Description**: * Test base for {@link SparkExecutionOperator} tests.


### Public Methods:

- **setUp()**: Returns `void`
- **getSC()**: Returns `JavaSparkContext`

---

## ChannelFactory

**Description**: * Utility to create {@link Channel}s in tests.


### Public Methods:

- **setUp()**: Returns `void`

---

# wayang-profiler

## DataGenerators

**Description**: * Utility to create common data generators.

- **Extends**: Supplier

---

## DiskProfiler

**Description**: * Profiles the reading and writing speed to some directory.


### Public Methods:

- **profile()**: Returns `String`

### Private Methods:

- **profileWriting()**: Returns `long`
- **profileReading()**: Returns `long`

---

## BinaryOperatorProfiler

**Description**: * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with two inputs and one output.

- **Extends**: SparkOperatorProfiler

---

## JavaCollectionSourceProfiler

**Description**: * {@link OperatorProfiler} for {@link JavaCollectionSource}s.

- **Extends**: SourceProfiler

### Fields:

- **sourceCollection**: `Collection<Object>` (private)

### Private Methods:

- **createOperator()**: Returns `JavaCollectionSource`

---

## JavaTextFileSourceProfiler

**Description**: * {@link OperatorProfiler} for sources.

- **Extends**: SourceProfiler

### Fields:

- **tempFile**: `File` (private)

---

## OperatorProfiler

### Fields:

- **cpuMhz**: `int` (public)
- **inputCardinalities**: `List<Long>` (private)

### Public Methods:

- **prepare()**: Returns `void`
- **run()**: Returns `Result`
- **getOperator()**: Returns `JavaExecutionOperator`
- **getInputCardinalities()**: Returns `List<Long>`
- **getOutputCardinality()**: Returns `long`
- **getDiskBytes()**: Returns `long`
- **getNetworkBytes()**: Returns `long`
- **getCpuCycles()**: Returns `long`
- **toString()**: Returns `String`
- **getCsvHeader()**: Returns `String`
- **toCsvString()**: Returns `String`

### Private Methods:

- **calculateCpuCycles()**: Returns `long`

---

## OperatorProfilers

**Description**: * Utilities to create {@link SparkOperatorProfiler} instances.


---

## Profiler

**Description**: * Utility to support finding reasonable {@link LoadProfileEstimator}s for {@link JavaExecutionOperator}s.


---

## SinkProfiler

**Description**: * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with one input and no outputs.

- **Extends**: SparkOperatorProfiler

---

## SourceProfiler
- **Extends**: OperatorProfiler

### Fields:

- **outputChannelInstance**: `JavaChannelInstance` (private)

### Public Methods:

- **prepare()**: Returns `void`

---

## UnaryOperatorProfiler

**Description**: * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.

- **Extends**: OperatorProfiler

### Public Methods:

- **prepare()**: Returns `void`
- **executeOperator()**: Returns `long`
- **getOperator()**: Returns `JavaExecutionOperator`

---

## DynamicEstimationContext

**Description**: * {@link EstimationContext} implementation for {@link DynamicLoadEstimator}s.

- **Implements**: EstimationContext

### Public Methods:

- **getDoubleProperty()**: Returns `double`
- **getNumExecutions()**: Returns `int`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getIndividual()**: Returns `Individual`

---

## DynamicLoadEstimator

**Description**: * Adjustable {@link LoadProfileEstimator} implementation.

- **Extends**: LoadEstimator

### Public Methods:

- **getVariable()**: Returns `double`
- **calculate()**: Returns `LoadEstimate`
- **toMathEx()**: Returns `String`
- **getEmployedVariables()**: Returns `Collection<Variable>`

---

## DynamicLoadProfileEstimator

**Description**: * Adjustable {@link LoadProfileEstimator} implementation.

- **Implements**: LoadProfileEstimator

### Public Methods:

- **estimate()**: Returns `LoadProfile`
- **nest()**: Returns `void`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **toJsonConfig()**: Returns `String`
- **getEmployedVariables()**: Returns `Collection<Variable>`

---

## DynamicLoadProfileEstimators

**Description**: * Utility to create {@link DynamicLoadProfileEstimator}s.


### Public Methods:

- **estimate()**: Returns `LoadProfile`
- **getEmployedVariables()**: Returns `Collection<Variable>`
- **calculate()**: Returns `LoadEstimate`

---

## GeneticOptimizer

**Description**: * Implementation of the genetic optimization technique for finding good {@link LoadProfileEstimator}s.


### Fields:

- **runtimeSum**: `long` (private)

### Public Methods:

- **createInitialPopulation()**: Returns `List<Individual>`
- **updateFitness()**: Returns `void`
- **evolve()**: Returns `List<Individual>`
- **getActivatedGenes()**: Returns `Bitmask`
- **getData()**: Returns `Collection<PartialExecution>`
- **getConfiguration()**: Returns `Configuration`
- **getOptimizationSpace()**: Returns `OptimizationSpace`
- **getObservations()**: Returns `Collection<PartialExecution>`
- **calculateObservationBasedWeight()**: Returns `double`
- **calculateRuntimeBasedWeight()**: Returns `double`

### Private Methods:

- **adjustOrPutValue()**: Returns `void`
- **updateFitnessOf()**: Returns `void`

---

## GeneticOptimizerApp

**Description**: * This app tries to infer good {@link LoadProfileEstimator}s for {@link ExecutionOperator}s using data from an
 * {@link ExecutionLog}.


### Public Methods:

- **run()**: Returns `void`

### Private Methods:

- **checkConfidence()**: Returns `boolean`
- **checkNonEmptyCardinalities()**: Returns `boolean`
- **checkEstimatorTemplates()**: Returns `boolean`
- **checkSpread()**: Returns `boolean`
- **instrument()**: Returns `void`
- **printResults()**: Returns `void`
- **printLearnedConfiguration()**: Returns `void`
- **createOptimizer()**: Returns `GeneticOptimizer`
- **getLoadProfileEstimatorTemplateKeys()**: Returns `Set<String>`
- **binByExecutionTime()**: Returns `Collection<PartialExecution>`

---

## Individual

**Description**: * Context for the optimization of {@link LoadProfileEstimator}s.


### Fields:

- **fitnessAccumulator**: `double` (private)
- **weightAccumulator**: `double` (private)

### Public Methods:

- **setGene()**: Returns `void`
- **mutate()**: Returns `Individual`
- **crossOver()**: Returns `Individual`
- **updateMaturity()**: Returns `void`
- **updateFitness()**: Returns `double`
- **getFitness()**: Returns `double`

### Private Methods:

- **updateMaturity()**: Returns `void`
- **calculateRelativeDelta()**: Returns `double`
- **calculateAbsolutePartialFitness()**: Returns `double`
- **estimateTime()**: Returns `TimeEstimate`

---

## LogEvaluator

**Description**: * Evaluates a {@link Configuration} on a {@link ExecutionLog}.


### Fields:

- **sortCriterion**: `Comparator<PartialExecution>` (private)

### Private Methods:

- **runUserLoop()**: Returns `void`
- **printPartialExecutions()**: Returns `void`
- **print()**: Returns `void`
- **printStatistics()**: Returns `void`
- **modifyFilters()**: Returns `void`
- **modifySorting()**: Returns `void`
- **createPartialExecutionStream()**: Returns `Stream<PartialExecution>`

---

## OptimizationSpace

**Description**: * Context for the optimization of {@link LoadProfileEstimator}s.


### Public Methods:

- **getOrCreateVariable()**: Returns `Variable`
- **getVariable()**: Returns `Variable`
- **getVariable()**: Returns `Variable`
- **createRandomIndividual()**: Returns `Individual`
- **getVariables()**: Returns `List<Variable>`
- **getNumDimensions()**: Returns `int`

---

## Variable

**Description**: * A variable that can be altered by an optimization algorithm.


### Public Methods:

- **getId()**: Returns `String`
- **getValue()**: Returns `double`
- **createRandomValue()**: Returns `double`
- **mutate()**: Returns `double`
- **setRandomValue()**: Returns `void`
- **getIndex()**: Returns `int`
- **toString()**: Returns `String`

---

## Battle

---

## ReservoirSampler
- **Implements**: Sampler

### Public Methods:

- **sample()**: Returns `List<T>`

---

## Sampler

---

## TournamentSampler

**Description**: * Sampling strategy that simulates a tournament between elements.

- **Implements**: Sampler

### Public Methods:

- **sample()**: Returns `List<T>`

---

## Main

**Description**: * Starts a profiling run of Spark.


---

## SparkCollectionSourceProfiler

**Description**: * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.

- **Extends**: SparkSourceProfiler

### Public Methods:

- **cleanUp()**: Returns `void`

---

## SparkOperatorProfiler

### Public Methods:

- **prepare()**: Returns `void`
- **run()**: Returns `Result`
- **cleanUp()**: Returns `void`
- **getInputCardinalities()**: Returns `List<Long>`
- **getOutputCardinality()**: Returns `long`
- **getDiskBytes()**: Returns `long`
- **getNetworkBytes()**: Returns `long`
- **getCpuCycles()**: Returns `long`
- **toString()**: Returns `String`
- **getCsvHeader()**: Returns `String`
- **toCsvString()**: Returns `String`

### Private Methods:

- **waitAndQueryMetricAverage()**: Returns `double`

---

## SparkSourceProfiler
- **Extends**: SparkOperatorProfiler

---

## SparkTextFileSourceProfiler

**Description**: * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.

- **Extends**: SparkSourceProfiler

### Public Methods:

- **cleanUp()**: Returns `void`

---

## SparkUnaryOperatorProfiler

**Description**: * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with one input and one output.

- **Extends**: SparkOperatorProfiler

---

## ProfilingUtils

**Description**: * Utilities to fake Wayang internals etc..


---

## RrdAccessor

**Description**: * Utility to read from an RRD file.

- **Implements**: AutoCloseable

### Public Methods:

- **query()**: Returns `double`
- **getLastUpdateMillis()**: Returns `long`
- **close()**: Returns `void`

---

# wayang-resources

# wayang-tests-integration

## FlinkIntegrationIT

**Description**: * Test the Spark integration with Wayang.


### Public Methods:

- **testReadAndWrite()**: Returns `void`
- **testReadAndTransformAndWrite()**: Returns `void`
- **testCartesianOperator()**: Returns `void`
- **testCoGroupOperator()**: Returns `void`
- **testCollectionSource()**: Returns `void`
- **testCountOperator()**: Returns `void`
- **testDistinctOperator()**: Returns `void`
- **testFilterOperator()**: Returns `void`
- **testFlapMapOperator()**: Returns `void`
- **testJoinOperator()**: Returns `void`
- **testReduceByOperator()**: Returns `void`
- **testSortOperator()**: Returns `void`
- **testTextFileSink()**: Returns `void`
- **testUnionOperator()**: Returns `void`
- **testZipWithIdOperator()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration1()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration2()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration3()**: Returns `void`
- **testMultiSourceAndMultiSink()**: Returns `void`
- **testMultiSourceAndHoleAndMultiSink()**: Returns `void`
- **testGlobalMaterializedGroup()**: Returns `void`
- **testIntersect()**: Returns `void`
- **testPageRankWithGraphBasic()**: Returns `void`
- **testMapPartitions()**: Returns `void`
- **testZipWithId()**: Returns `void`
- **testDiverseScenario1()**: Returns `void`
- **testDiverseScenario2()**: Returns `void`
- **testDiverseScenario3()**: Returns `void`
- **testDiverseScenario4()**: Returns `void`
- **testSample()**: Returns `void`

### Private Methods:

- **makeContext()**: Returns `WayangContext`
- **makeAndRun()**: Returns `void`
- **makeList()**: Returns `List<String>`

---

## FullIntegrationIT

**Description**: * Test the Java integration with Wayang.


### Fields:

- **configuration**: `Configuration` (private)

### Public Methods:

- **setUp()**: Returns `void`
- **testReadAndWrite()**: Returns `void`
- **testReadAndWriteCrossPlatform()**: Returns `void`
- **testReadAndTransformAndWrite()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration1()**: Returns `void`
- **testMultiSourceAndMultiSink()**: Returns `void`
- **testMultiSourceAndHoleAndMultiSink()**: Returns `void`
- **testGlobalMaterializedGroup()**: Returns `void`
- **testIntersect()**: Returns `void`
- **testRepeat()**: Returns `void`
- **testPageRankWithGraphBasic()**: Returns `void`
- **testMapPartitions()**: Returns `void`
- **testZipWithId()**: Returns `void`
- **testDiverseScenario1()**: Returns `void`
- **testDiverseScenario2()**: Returns `void`
- **testDiverseScenario3()**: Returns `void`
- **testDiverseScenario4()**: Returns `void`
- **testSimpleSingleStageLoop()**: Returns `void`
- **testSimpleMultiStageLoop()**: Returns `void`
- **testSimpleSample()**: Returns `void`
- **testCurrentIterationNumber()**: Returns `void`
- **testCurrentIterationNumberWithTooFewExpectedIterations()**: Returns `void`
- **testGroupByOperator()**: Returns `void`
- **testSqlite3Scenario1()**: Returns `void`
- **testSqlite3Scenario2()**: Returns `void`
- **testSqlite3Scenario3()**: Returns `void`

---

## GiraphIntegrationIT

**Description**: * Integration tests for the integration of Giraph with Wayang.


### Public Methods:

- **testPageRankWithJava()**: Returns `void`
- **testPageRankWithoutGiraph()**: Returns `void`

### Private Methods:

- **check()**: Returns `void`

---

## JavaIntegrationIT

**Description**: * Test the Java integration with Wayang.


### Fields:

- **allowedInts**: `Set<Integer>` (private)
- **coefficient**: `int` (private)

### Public Methods:

- **testReadAndWrite()**: Returns `void`
- **testReadAndTransformAndWrite()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration1()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration2()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration3()**: Returns `void`
- **testMultiSourceAndMultiSink()**: Returns `void`
- **testMultiSourceAndHoleAndMultiSink()**: Returns `void`
- **testGlobalMaterializedGroup()**: Returns `void`
- **testIntersect()**: Returns `void`
- **testRepeat()**: Returns `void`
- **testPageRankWithGraphBasic()**: Returns `void`
- **testPageRankWithJavaGraph()**: Returns `void`
- **testMapPartitions()**: Returns `void`
- **testZipWithId()**: Returns `void`
- **testDiverseScenario1()**: Returns `void`
- **testDiverseScenario2()**: Returns `void`
- **testDiverseScenario3()**: Returns `void`
- **testDiverseScenario4()**: Returns `void`
- **testSimpleLoop()**: Returns `void`
- **testSample()**: Returns `void`
- **testLargerSample()**: Returns `void`
- **testCurrentIterationNumber()**: Returns `void`
- **testCurrentIterationNumberWithTooFewExpectedIterations()**: Returns `void`
- **testBroadcasts()**: Returns `void`
- **open()**: Returns `void`
- **test()**: Returns `boolean`
- **testBroadcasts2()**: Returns `void`
- **open()**: Returns `void`
- **apply()**: Returns `Integer`

---

## PostgresIntegrationIT

---

## RegressionIT

**Description**: * This class hosts and documents some tests for bugs that we encountered. Ultimately, we want to avoid re-introducing
 * already encountered and fixed bugs.


### Public Methods:

- **testCollectionToRddAndBroadcast()**: Returns `void`

---

## SparkIntegrationIT

**Description**: * Test the Spark integration with Wayang.

- **Implements**: PredicateDescriptor

### Fields:

- **allowedInts**: `Set<Integer>` (private)

### Public Methods:

- **testReadAndWrite()**: Returns `void`
- **testReadAndTransformAndWrite()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration1()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration2()**: Returns `void`
- **testReadAndTransformAndWriteWithIllegalConfiguration3()**: Returns `void`
- **testMultiSourceAndMultiSink()**: Returns `void`
- **testMultiSourceAndHoleAndMultiSink()**: Returns `void`
- **testGlobalMaterializedGroup()**: Returns `void`
- **testIntersect()**: Returns `void`
- **testRepeat()**: Returns `void`
- **testPageRankWithGraphBasic()**: Returns `void`
- **testPageRankWithSparkGraph()**: Returns `void`
- **testMapPartitions()**: Returns `void`
- **testZipWithId()**: Returns `void`
- **testDiverseScenario1()**: Returns `void`
- **testDiverseScenario2()**: Returns `void`
- **testDiverseScenario3()**: Returns `void`
- **testDiverseScenario4()**: Returns `void`
- **testSimpleLoop()**: Returns `void`
- **testSample()**: Returns `void`
- **testSampleInLoop()**: Returns `void`
- **testCurrentIterationNumber()**: Returns `void`
- **testCurrentIterationNumberWithTooFewExpectedIterations()**: Returns `void`
- **testBroadcasts()**: Returns `void`
- **open()**: Returns `void`
- **test()**: Returns `boolean`

---

## WayangPlans

**Description**: * Provides plans that can be used for integration testing.

- **Implements**: FunctionDescriptor

### Fields:

- **increment**: `int` (private)

### Public Methods:

- **open()**: Returns `void`
- **apply()**: Returns `Integer`
- **open()**: Returns `void`
- **open()**: Returns `void`

---

## WayangPlansOperators

**Description**: * Provides plans that can be used for integration testing..

- **Extends**: WayangPlans

---

## WordCountIT

**Description**: * Word count integration test. Besides going through different {@link Platform} combinations, each test addresses a different
 * way of specifying the target {@link Platform}s.


### Public Methods:

- **testOnJava()**: Returns `void`
- **testOnSpark()**: Returns `void`
- **testOnSparkToJava()**: Returns `void`
- **testOnJavaToSpark()**: Returns `void`
- **testOnJavaAndSpark()**: Returns `void`

---

## MyMadeUpPlatform

**Description**: * Dummy {@link Platform} that does not provide any {@link Mapping}s.

- **Extends**: Platform
- **Implements**: Plugin

### Public Methods:

- **configureDefaults()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

---

## GraphChiIntegrationIT

**Description**: * Integration tests for the integration of GraphChi with Wayang.


### Public Methods:

- **testPageRankWithJava()**: Returns `void`
- **testPageRankWithSpark()**: Returns `void`
- **testPageRankWithoutGraphChi()**: Returns `void`

### Private Methods:

- **check()**: Returns `void`

---
