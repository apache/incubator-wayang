<!--

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

-->
# wayang-api

## # wayang-api

### Fields:

- **empty()**: Returns `void`

### Public Methods:

- **empty()**: Returns `void`

## 

### Fields:

- **socket**: `Socket` (private)
- **serializedUDF**: `PythonCode` (private)
- **input**: `Iterable<Input>` (private)
- **send()**: Returns `void`
- **writeUDF()**: Returns `void`
- **writeIteratorToStream()**: Returns `void`
- **write()**: Returns `void`
- **writeBytes()**: Returns `void`
- **writeUTF()**: Returns `void`
- **writeKeyValue()**: Returns `void`

### Public Methods:

- **send()**: Returns `void`
- **writeUDF()**: Returns `void`
- **writeIteratorToStream()**: Returns `void`
- **write()**: Returns `void`
- **writeBytes()**: Returns `void`
- **writeUTF()**: Returns `void`
- **writeKeyValue()**: Returns `void`

## 

### Fields:

- **iterator**: `ReaderIterator<Output>` (private)
- **getIterable()**: Returns `Iterable<Output>`
- **print()**: Returns `void`

### Public Methods:

- **getIterable()**: Returns `Iterable<Output>`
- **print()**: Returns `void`

## 

### Fields:

- **process**: `Thread` (private)
- **socket**: `Socket` (private)
- **serverSocket**: `ServerSocket` (private)
- **ready**: `boolean` (private)
- **configuration**: `Configuration` (private)
- **getProcess()**: Returns `Thread`
- **getSocket()**: Returns `Socket`
- **isReady()**: Returns `boolean`
- **close()**: Returns `void`

### Public Methods:

- **getProcess()**: Returns `Thread`
- **getSocket()**: Returns `Socket`
- **isReady()**: Returns `boolean`
- **close()**: Returns `void`

## 

### Fields:

- **serializedUDF**: `PythonCode` (private)
- **inputIterator**: `Iterable<Input>` (private)
- **execute()**: Returns `Iterable<Output>`

### Public Methods:

- **execute()**: Returns `Iterable<Output>`

## 

### Fields:

- **Implements**: Iterator
- **hasNext()**: Returns `boolean`
- **next()**: Returns `Output`
- **read()**: Returns `Output`

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `Output`
- **read()**: Returns `Output`

## 

### Fields:

- **Implements**: Serializable

## 

### Fields:

- **Implements**: FunctionDescriptor
- **serializedUDF**: `PythonCode` (private)
- **apply()**: Returns `Iterable<Output>`

### Public Methods:

- **apply()**: Returns `Iterable<Output>`

## 

### Fields:

- **Extends**: FunctionDescriptor

## 

## 

### Fields:

- **wayangPlan**: `WayangPlan` (private)
- **wayangContext**: `WayangContext` (private)
- **createOperatorByType()**: Returns `OperatorBase`
- **getWayangContext()**: Returns `WayangContext`
- **getWayangPlan()**: Returns `WayangPlan`
- **buildContext()**: Returns `WayangContext`
- **buildPlan()**: Returns `WayangPlan`

### Public Methods:

- **createOperatorByType()**: Returns `OperatorBase`
- **getWayangContext()**: Returns `WayangContext`
- **getWayangPlan()**: Returns `WayangPlan`
- **buildContext()**: Returns `WayangContext`
- **buildPlan()**: Returns `WayangPlan`

## 

### Fields:

- **planFromFile()**: Returns `String`
- **planFromMessage()**: Returns `String`
- **all()**: Returns `String`
- **createOperatorByType()**: Returns `OperatorBase`
- **buildContext()**: Returns `WayangContext`
- **buildPlan()**: Returns `WayangPlan`

### Public Methods:

- **planFromFile()**: Returns `String`
- **planFromMessage()**: Returns `String`
- **all()**: Returns `String`
- **createOperatorByType()**: Returns `OperatorBase`
- **buildContext()**: Returns `WayangContext`
- **buildPlan()**: Returns `WayangPlan`

## 

### Fields:

**Description**: * Test suite for the Java API.
- **Implements**: FunctionDescriptor
- **sqlite3Configuration**: `Configuration` (private)
- **offset**: `int` (private)
- **selectors**: `Collection<Character>` (private)
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

## 

### Fields:

- **Implements**: Convention
- **getInterface()**: Returns `Class`
- **getName()**: Returns `String`
- **canConvertConvention()**: Returns `boolean`
- **useAbstractConvertersForConversion()**: Returns `boolean`
- **getTraitDef()**: Returns `RelTraitDef`
- **satisfies()**: Returns `boolean`
- **register()**: Returns `void`
- **toString()**: Returns `String`

### Public Methods:

- **getInterface()**: Returns `Class`
- **getName()**: Returns `String`
- **canConvertConvention()**: Returns `boolean`
- **useAbstractConvertersForConversion()**: Returns `boolean`
- **getTraitDef()**: Returns `RelTraitDef`
- **satisfies()**: Returns `boolean`
- **register()**: Returns `void`
- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: WayangRelNodeVisitor
- **Implements**: FunctionDescriptor
- **test()**: Returns `boolean`
- **visitCall()**: Returns `Boolean`
- **eval()**: Returns `boolean`
- **isGreaterThan()**: Returns `boolean`
- **isLessThan()**: Returns `boolean`
- **isEqualTo()**: Returns `boolean`

### Public Methods:

- **test()**: Returns `boolean`
- **visitCall()**: Returns `Boolean`
- **eval()**: Returns `boolean`
- **isGreaterThan()**: Returns `boolean`
- **isLessThan()**: Returns `boolean`
- **isEqualTo()**: Returns `boolean`

## 

### Fields:

- **Extends**: WayangRelNodeVisitor
- **Implements**: FunctionDescriptor
- **visitCall()**: Returns `Integer`
- **apply()**: Returns `Object`
- **apply()**: Returns `Record`

### Public Methods:

- **visitCall()**: Returns `Integer`
- **apply()**: Returns `Object`
- **apply()**: Returns `Record`

## 

### Fields:

- **Extends**: WayangRelNodeVisitor
- **apply()**: Returns `Record`

### Public Methods:

- **apply()**: Returns `Record`

## 

### Fields:

- **convert()**: Returns `Operator`

### Public Methods:

- **convert()**: Returns `Operator`

## 

### Fields:

- **Extends**: RelNode

## 

### Fields:

- **Extends**: WayangRelNodeVisitor

## 

### Fields:

**Description**: * Implementation of {@link Schema} that is backed by a JDBC data source.
- **Extends**: BiFunction
- **Implements**: Schema
- **isMutable()**: Returns `boolean`
- **snapshot()**: Returns `Schema`
- **getDataSource()**: Returns `DataSource`
- **getExpression()**: Returns `Expression`
- **getTableNames()**: Returns `Set<String>`
- **getTypeNames()**: Returns `Set<String>`
- **getSubSchemaNames()**: Returns `Set<String>`
- **create()**: Returns `Schema`

### Public Methods:

- **isMutable()**: Returns `boolean`
- **snapshot()**: Returns `Schema`
- **getDataSource()**: Returns `DataSource`
- **getExpression()**: Returns `Expression`
- **getTableNames()**: Returns `Set<String>`
- **getTypeNames()**: Returns `Set<String>`
- **getSubSchemaNames()**: Returns `Set<String>`
- **create()**: Returns `Schema`

## 

### Fields:

**Description**: * Queryable that gets its data from a table within a JDBC connection.
- **Extends**: AbstractQueryableTable
- **Implements**: TranslatableTable, ScannableTable, ModifiableTable
- **toString()**: Returns `String`
- **getRowType()**: Returns `RelDataType`
- **tableName()**: Returns `SqlIdentifier`
- **toRel()**: Returns `RelNode`
- **toModificationRel()**: Returns `TableModify`
- **toString()**: Returns `String`
- **enumerator()**: Returns `Enumerator<T>`
- **supplyProto()**: Returns `RelProtoDataType`

### Public Methods:

- **toString()**: Returns `String`
- **getRowType()**: Returns `RelDataType`
- **tableName()**: Returns `SqlIdentifier`
- **toRel()**: Returns `RelNode`
- **toModificationRel()**: Returns `TableModify`
- **toString()**: Returns `String`
- **enumerator()**: Returns `Enumerator<T>`
- **supplyProto()**: Returns `RelProtoDataType`

## 

### Fields:

- **Extends**: ObjectArrayRowBuilder
- **Implements**: Function0
- **get()**: Returns `SqlDialect`
- **get()**: Returns `DataSource`

### Public Methods:

- **get()**: Returns `SqlDialect`
- **get()**: Returns `DataSource`

## 

### Fields:

- **parseSql()**: Returns `SqlNode`
- **validate()**: Returns `SqlNode`
- **convert()**: Returns `RelNode`
- **optimize()**: Returns `RelNode`
- **convert()**: Returns `WayangPlan`
- **convert()**: Returns `WayangPlan`

### Public Methods:

- **parseSql()**: Returns `SqlNode`
- **validate()**: Returns `SqlNode`
- **convert()**: Returns `RelNode`
- **optimize()**: Returns `RelNode`
- **convert()**: Returns `WayangPlan`
- **convert()**: Returns `WayangPlan`

## 

### Fields:

- **Implements**: Program
- **run()**: Returns `RelNode`

### Public Methods:

- **run()**: Returns `RelNode`

## 

### Fields:

- **Extends**: Filter
- **Implements**: WayangRel
- **copy()**: Returns `Filter`
- **toString()**: Returns `String`

### Public Methods:

- **copy()**: Returns `Filter`
- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: Join
- **Implements**: WayangRel
- **copy()**: Returns `WayangJoin`
- **toString()**: Returns `String`

### Public Methods:

- **copy()**: Returns `WayangJoin`
- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: Project
- **Implements**: WayangRel
- **copy()**: Returns `WayangProject`
- **toString()**: Returns `String`

### Public Methods:

- **copy()**: Returns `WayangProject`
- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: RelNode

## 

### Fields:

- **Extends**: TableScan
- **Implements**: WayangRel
- **toString()**: Returns `String`
- **getQualifiedName()**: Returns `String`
- **getTableName()**: Returns `String`
- **getColumnNames()**: Returns `List<String>`

### Public Methods:

- **toString()**: Returns `String`
- **getQualifiedName()**: Returns `String`
- **getTableName()**: Returns `String`
- **getColumnNames()**: Returns `List<String>`

## 

### Fields:

- **Extends**: ConverterRule
- **convert()**: Returns `RelNode`
- **convert()**: Returns `RelNode`

### Public Methods:

- **convert()**: Returns `RelNode`
- **convert()**: Returns `RelNode`

## 

## 

### Fields:

- **Extends**: AbstractSchema
- **getSchemaName()**: Returns `String`
- **snapshot()**: Returns `Schema`

### Public Methods:

- **getSchemaName()**: Returns `String`
- **snapshot()**: Returns `Schema`

## 

### Fields:

- **addTable()**: Returns `WayangSchemaBuilder`
- **build()**: Returns `WayangSchema`

### Public Methods:

- **addTable()**: Returns `WayangSchemaBuilder`
- **build()**: Returns `WayangSchema`

## 

### Fields:

- **Extends**: AbstractTable
- **rowType**: `RelDataType` (private)
- **getTableName()**: Returns `String`
- **getRowType()**: Returns `RelDataType`
- **getStatistic()**: Returns `Statistic`

### Public Methods:

- **getTableName()**: Returns `String`
- **getRowType()**: Returns `RelDataType`
- **getStatistic()**: Returns `Statistic`

## 

### Fields:

- **rowCount**: `long` (private)
- **addField()**: Returns `WayangTableBuilder`
- **withRowCount()**: Returns `WayangTableBuilder`
- **build()**: Returns `WayangTable`

### Public Methods:

- **addField()**: Returns `WayangTableBuilder`
- **withRowCount()**: Returns `WayangTableBuilder`
- **build()**: Returns `WayangTable`

## 

### Fields:

- **Implements**: Statistic
- **isKey()**: Returns `boolean`

### Public Methods:

- **isKey()**: Returns `boolean`

## 

### Fields:

- **configuration**: `Configuration` (private)
- **json**: `JSONObject` (private)
- **setProperties()**: Returns `Configuration`
- **getFsPath()**: Returns `String`
- **getSeparator()**: Returns `String`

### Public Methods:

- **setProperties()**: Returns `Configuration`
- **getFsPath()**: Returns `String`
- **getSeparator()**: Returns `String`

## 

## 

### Fields:

- **executeSql()**: Returns `Collection<Record>`

### Public Methods:

- **executeSql()**: Returns `Collection<Record>`

## 

### Fields:

**Description**: * Based on Calcite's CSV enumerator.

## 

### Fields:

- **Extends**: UnarySource
- **Implements**: JavaExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `String`
- **createStream()**: Returns `Stream<T>`
- **parseLine()**: Returns `T`
- **advance()**: Returns `void`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `String`
- **createStream()**: Returns `Stream<T>`
- **parseLine()**: Returns `T`
- **advance()**: Returns `void`

## 

## 

## 

### Fields:

- **test_simple_sql()**: Returns `void`
- **print()**: Returns `void`
- **print()**: Returns `void`

### Public Methods:

- **test_simple_sql()**: Returns `void`
- **print()**: Returns `void`
- **print()**: Returns `void`

---

# wayang-assembly

---

# wayang-benchmark

## # wayang-benchmark

### Fields:

- **Implements**: Serializable
- **iterator()**: Returns `Iterator<CharSequence>`

### Public Methods:

- **iterator()**: Returns `Iterator<CharSequence>`

## 

### Fields:

**Description**: * This class executes a stochastic gradient descent optimization on Apache Wayang (incubating).
- **Implements**: FunctionDescriptor
- **accuracy**: `double` (public)
- **max_iterations**: `int` (public)
- **current_iteration**: `int` (private)
- **open()**: Returns `void`
- **open()**: Returns `void`
- **open()**: Returns `void`
- **test()**: Returns `boolean`
- **open()**: Returns `void`

### Public Methods:

- **open()**: Returns `void`
- **open()**: Returns `void`
- **open()**: Returns `void`
- **test()**: Returns `boolean`
- **open()**: Returns `void`

## 

### Fields:

**Description**: * This class executes a stochastic gradient descent optimization on Apache Wayang (incubating), just like {@link SGDImpl}. However,
- **Implements**: FunctionDescriptor
- **open()**: Returns `void`

### Public Methods:

- **open()**: Returns `void`

## 

### Fields:

- **Implements**: a 128

## 

### Fields:

- **Implements**: Writable, Serializable
- **hi8**: `long` (private)
- **lo8**: `long` (private)
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

## 

### Fields:

**Description**: * Example Apache Wayang (incubating) App that does a word count -- the Hello World of Map/Reduce-like systems.

## 

### Fields:

**Description**: * A tuple of the lineitem table.
- **Implements**: Serializable
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
- **parse()**: Returns `LineItemTuple`

### Public Methods:

- **parse()**: Returns `LineItemTuple`

## 

### Fields:

**Description**: * Grouping key used in Query 1.
- **Implements**: Serializable
- **L_RETURNFLAG**: `char` (public)
- **L_LINESTATUS**: `char` (public)
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

## 

### Fields:

**Description**: * Tuple that is returned by Query 1.
- **Implements**: Serializable
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
- **toString()**: Returns `String`

### Public Methods:

- **toString()**: Returns `String`

## 

### Fields:

**Description**: * Test suited for {@link LineItemTuple}.
- **testParser()**: Returns `void`
- **toDateInteger()**: Returns `int`

### Public Methods:

- **testParser()**: Returns `void`
- **toDateInteger()**: Returns `int`

---

# wayang-commons

## # wayang-commons

### Fields:

**Description**: * Register for plugins in the module.

## 

### Fields:

**Description**: * Represents a {@link Channel} that is realized via a file/set of files.
- **Extends**: Channel
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

## 

### Fields:

**Description**: * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.
- **Implements**: Serializable, Copyable
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

## 

### Fields:

**Description**: * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
- **Implements**: Serializable
- **field0**: `T0` (public)
- **field1**: `T1` (public)
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **getField1()**: Returns `T1`

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **getField1()**: Returns `T1`

## 

### Fields:

**Description**: * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
- **Implements**: Serializable
- **field0**: `T0` (public)
- **field1**: `T1` (public)
- **field2**: `T2` (public)
- **field3**: `T3` (public)
- **field4**: `T4` (public)
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **getField1()**: Returns `T1`
- **getField2()**: Returns `T2`
- **getField3()**: Returns `T3`
- **getField4()**: Returns `T4`

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **getField1()**: Returns `T1`
- **getField2()**: Returns `T2`
- **getField3()**: Returns `T3`
- **getField4()**: Returns `T4`

## 

### Fields:

**Description**: * This descriptor pertains to projections. It takes field names of the input type to describe the projection.
- **Extends**: TransformationDescriptor
- **Implements**: FunctionDescriptor
- **fieldNames**: `List<String>` (private)
- **field**: `Field` (private)
- **getFieldNames()**: Returns `List<String>`
- **apply()**: Returns `Output`
- **apply()**: Returns `Record`

### Public Methods:

- **getFieldNames()**: Returns `List<String>`
- **apply()**: Returns `Output`
- **apply()**: Returns `Record`

## 

### Fields:

**Description**: * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
- **Extends**: ReplacementSubplanFactory
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

## 

### Fields:

**Description**: * Register for the components provided in the basic plugin.

## 

### Fields:

**Description**: * This mapping translates the {@link GroupByOperator} into the {@link MaterializedGroupByOperator}.
- **Extends**: ReplacementSubplanFactory
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

## 

### Fields:

**Description**: * This {@link Mapping} translates a {@link PageRankOperator} into a {@link Subplan} of basic {@link Operator}s.
- **Implements**: Mapping
- **initialRank**: `Float` (private)
- **minRank**: `float` (private)
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **open()**: Returns `void`
- **open()**: Returns `void`
- **createTransformation()**: Returns `PlanTransformation`
- **createPattern()**: Returns `SubplanPattern`
- **createReplacementFactory()**: Returns `ReplacementSubplanFactory`
- **createPageRankSubplan()**: Returns `Operator`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **open()**: Returns `void`
- **open()**: Returns `void`
- **createTransformation()**: Returns `PlanTransformation`
- **createPattern()**: Returns `SubplanPattern`
- **createReplacementFactory()**: Returns `ReplacementSubplanFactory`
- **createPageRankSubplan()**: Returns `Operator`

## 

### Fields:

**Description**: * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
- **Extends**: ReplacementSubplanFactory
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createTransformation()**: Returns `PlanTransformation`
- **createPattern()**: Returns `SubplanPattern`
- **createReplacementFactory()**: Returns `ReplacementSubplanFactory`
- **createLoopOperatorSubplan()**: Returns `Subplan`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createTransformation()**: Returns `PlanTransformation`
- **createPattern()**: Returns `SubplanPattern`
- **createReplacementFactory()**: Returns `ReplacementSubplanFactory`
- **createLoopOperatorSubplan()**: Returns `Subplan`

## 

### Fields:

**Description**: * This operator returns the cartesian product of elements of input datasets.
- **Extends**: BinaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator groups both inputs by some key and then matches groups with the same key. If a key appears in only
- **Extends**: BinaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This source takes as input a Java {@link java.util.Collection}.
- **Extends**: UnarySource
- **Implements**: ElementaryOperator
- **getCollection()**: Returns `Collection<T>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getCollection()**: Returns `Collection<T>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator returns the count of elements in this stream.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator returns the distinct elements in this dataset.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator has three inputs and two outputs.
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator, LoopHeadOperator
- **state**: `State` (private)
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
- **initializeSlots()**: Returns `void`

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
- **initializeSlots()**: Returns `void`

## 

### Fields:

**Description**: * This operator returns a new dataset after filtering by applying predicateDescriptor.
- **Extends**: UnaryToUnaryOperator
- **Implements**: org
- **getPredicateDescriptor()**: Returns `PredicateDescriptor<Type>`
- **getType()**: Returns `DataSetType<Type>`
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **getPredicateDescriptor()**: Returns `PredicateDescriptor<Type>`
- **getType()**: Returns `DataSetType<Type>`
- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * A flatmap operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
- **Extends**: UnaryToUnaryOperator
- **Implements**: org
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * This operator groups the elements of a data set into a single data quantum.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator groups the elements of a data set and aggregates the groups.
- **Extends**: UnaryToUnaryOperator
- **getType()**: Returns `DataSetType<Type>`
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This is the auxiliary GroupBy operator, i.e., it behaves differently depending on its context. If it is followed
- **Extends**: UnaryToUnaryOperator

## 

### Fields:

**Description**: * This operator returns the set intersection of elements of input datasets.
- **Extends**: BinaryToUnaryOperator
- **getType()**: Returns `DataSetType<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator returns the cartesian product of elements of input datasets.
- **Extends**: BinaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This sink executes a callback on each received data unit into a Java {@link Collection}.
- **Extends**: UnarySink
- **setCollector()**: Returns `LocalCallbackSink<T>`
- **getCallback()**: Returns `Consumer<T>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **setCollector()**: Returns `LocalCallbackSink<T>`
- **getCallback()**: Returns `Consumer<T>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator has three inputs and three outputs.
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator, LoopHeadOperator
- **state**: `State` (private)
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
- **initializeSlots()**: Returns `void`

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
- **initializeSlots()**: Returns `void`

## 

### Fields:

**Description**: * A map operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator takes as input potentially multiple input data quanta and outputs multiple input data quanta.
- **Extends**: UnaryToUnaryOperator
- **Implements**: org
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * This operator collocates the data units in a data set w.r.t. a key function.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This {@link UnarySink} writes all incoming data quanta to a Object file.
- **Extends**: UnarySink

## 

### Fields:

**Description**: * This source reads a text file and outputs the lines as data units.
- **Extends**: UnarySource
- **Implements**: org
- **getInputUrl()**: Returns `String`
- **getTypeClass()**: Returns `Class<T>`
- **estimate()**: Returns `CardinalityEstimate`
- **estimateBytesPerLine()**: Returns `OptionalDouble`

### Public Methods:

- **getInputUrl()**: Returns `String`
- **getTypeClass()**: Returns `Class<T>`
- **estimate()**: Returns `CardinalityEstimate`
- **estimateBytesPerLine()**: Returns `OptionalDouble`

## 

### Fields:

**Description**: * {@link Operator} for the PageRank algorithm. It takes as input a list of directed edges, whereby each edge
- **Extends**: UnaryToUnaryOperator
- **getNumIterations()**: Returns `int`
- **getDampingFactor()**: Returns `float`
- **getGraphDensity()**: Returns `ProbabilisticDoubleInterval`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getNumIterations()**: Returns `int`
- **getDampingFactor()**: Returns `float`
- **getGraphDensity()**: Returns `ProbabilisticDoubleInterval`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator groups the elements of a data set and aggregates the groups.
- **Extends**: UnaryToUnaryOperator
- **getType()**: Returns `DataSetType<Type>`
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator is context dependent: after a {@link GroupByOperator}, it is meant to be a {@link ReduceByOperator};
- **Extends**: UnaryToUnaryOperator
- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getReduceDescriptor()**: Returns `ReduceDescriptor<Type>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This {@link Operator} repeats a certain subplan of {@link Operator}s for a given number of times.
- **Extends**: OperatorBase
- **Implements**: ElementaryOperator, LoopHeadOperator
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
- **initializeSlots()**: Returns `void`

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
- **initializeSlots()**: Returns `void`

## 

### Fields:

**Description**: * A random sample operator randomly selects its inputs from the input slot and pushes that element to the output slot.
- **Extends**: UnaryToUnaryOperator
- **sampleMethod**: `Methods` (private)
- **getType()**: Returns `DataSetType<Type>`
- **getDatasetSize()**: Returns `long`
- **setDatasetSize()**: Returns `void`
- **getSampleMethod()**: Returns `Methods`
- **setSampleMethod()**: Returns `void`
- **setSeedFunction()**: Returns `void`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getType()**: Returns `DataSetType<Type>`
- **getDatasetSize()**: Returns `long`
- **setDatasetSize()**: Returns `void`
- **getSampleMethod()**: Returns `Methods`
- **setSampleMethod()**: Returns `void`
- **setSeedFunction()**: Returns `void`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operator sorts the elements in this dataset.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * {@link UnarySource} that provides the tuples from a database table.
- **Extends**: UnarySource
- **getTableName()**: Returns `String`

### Public Methods:

- **getTableName()**: Returns `String`

## 

### Fields:

**Description**: * This {@link UnarySink} writes all incoming data quanta to a text file.
- **Extends**: UnarySink

## 

### Fields:

**Description**: * This source reads a text file and outputs the lines as data units.
- **Extends**: UnarySource
- **Implements**: org
- **getInputUrl()**: Returns `String`
- **getEncoding()**: Returns `String`
- **estimate()**: Returns `CardinalityEstimate`
- **estimateBytesPerLine()**: Returns `OptionalDouble`

### Public Methods:

- **getInputUrl()**: Returns `String`
- **getEncoding()**: Returns `String`
- **estimate()**: Returns `CardinalityEstimate`
- **estimateBytesPerLine()**: Returns `OptionalDouble`

## 

### Fields:

**Description**: * This {@link Operator} creates the union (bag semantics) of two .
- **Extends**: BinaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * This operators attaches a unique ID to each input data quantum.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

- **Implements**: Plugin
- **setProperties()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`

### Public Methods:

- **setProperties()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`

## 

### Fields:

- **Implements**: Plugin
- **setProperties()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`

### Public Methods:

- **setProperties()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`

## 

### Fields:

**Description**: * This is a specific {@link BasicDataUnitType} for {@link Record}s. In particular, it adds schema information.
- **Extends**: BasicDataUnitType
- **isSupertypeOf()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getIndex()**: Returns `int`

### Public Methods:

- **isSupertypeOf()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getIndex()**: Returns `int`

## 

### Fields:

**Description**: * Tests for the {@link ProjectionDescriptor}.
- **string**: `String` (public)
- **integer**: `int` (public)
- **testPojoImplementation()**: Returns `void`
- **testRecordImplementation()**: Returns `void`

### Public Methods:

- **testPojoImplementation()**: Returns `void`
- **testRecordImplementation()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link ReduceByMapping}.
- **testMapping()**: Returns `void`

### Public Methods:

- **testMapping()**: Returns `void`

## 

### Fields:

**Description**: * Tests for the {@link MaterializedGroupByOperator}.
- **testConnectingToMap()**: Returns `void`

### Public Methods:

- **testConnectingToMap()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link TextFileSource}.
- **testCardinalityEstimation()**: Returns `void`

### Public Methods:

- **testCardinalityEstimation()**: Returns `void`

## 

### Fields:

**Description**: * Dummy sink for testing purposes.
- **Extends**: UnarySink
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * Dummy source for testing purposes.
- **Extends**: UnarySource
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **setCardinalityEstimators()**: Returns `void`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **setCardinalityEstimators()**: Returns `void`

## 

### Fields:

**Description**: * Tests for the {@link RecordType}.
- **testSupertype()**: Returns `void`

### Public Methods:

- **testSupertype()**: Returns `void`

## 

### Fields:

**Description**: * Describes both the configuration of a {@link WayangContext} and {@link Job}s.
- **costSquasherProvider**: `ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>>` (private)
- **platformProvider**: `ExplicitCollectionProvider<Platform>` (private)
- **mappingProvider**: `ExplicitCollectionProvider<Mapping>` (private)
- **channelConversionProvider**: `ExplicitCollectionProvider<ChannelConversion>` (private)
- **pruningStrategyClassProvider**: `CollectionProvider<Class<PlanEnumerationPruningStrategy>>` (private)
- **instrumentationStrategyProvider**: `ValueProvider<InstrumentationStrategy>` (private)
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
- **handleConfigurationFileEntry()**: Returns `void`

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
- **handleConfigurationFileEntry()**: Returns `void`

## 

### Fields:

**Description**: * Describes a job that is to be executed using Wayang.
- **Extends**: OneTimeExecutable
- **optimizationContext**: `DefaultOptimizationContext` (private)
- **crossPlatformExecutor**: `CrossPlatformExecutor` (private)
- **cardinalityEstimatorManager**: `CardinalityEstimatorManager` (private)
- **monitor**: `Monitor` (private)
- **planImplementation**: `PlanImplementation` (private)
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

## 

### Fields:

**Description**: * This is the entry point for users to work with Wayang.
- **cardinalityRepository**: `CardinalityRepository` (private)
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

## 

### Fields:

- **Implements**: Iterable
- **setParent()**: Returns `void`
- **provideAll()**: Returns `Collection<Value>`
- **iterator()**: Returns `Iterator<Value>`

### Public Methods:

- **setParent()**: Returns `void`
- **provideAll()**: Returns `Collection<Value>`
- **iterator()**: Returns `Iterator<Value>`

## 

### Fields:

**Description**: * Used by {@link Configuration}s to provide some value.
- **Extends**: ValueProvider
- **value**: `Value` (private)
- **setValue()**: Returns `void`

### Public Methods:

- **setValue()**: Returns `void`

## 

### Fields:

**Description**: * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
- **Extends**: CollectionProvider
- **addToWhitelist()**: Returns `boolean`
- **addAllToWhitelist()**: Returns `void`
- **addToBlacklist()**: Returns `boolean`
- **addAllToBlacklist()**: Returns `void`
- **provideAll()**: Returns `Collection<Value>`

### Public Methods:

- **addToWhitelist()**: Returns `boolean`
- **addAllToWhitelist()**: Returns `void`
- **addToBlacklist()**: Returns `boolean`
- **addAllToBlacklist()**: Returns `void`
- **provideAll()**: Returns `Collection<Value>`

## 

### Fields:

**Description**: * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
- **Extends**: CollectionProvider
- **provideAll()**: Returns `Collection<Value>`

### Public Methods:

- **provideAll()**: Returns `Collection<Value>`

## 

### Fields:

**Description**: * Implementation of {@link KeyValueProvider} that uses a {@link Function} to provide a value.
- **Extends**: KeyValueProvider

## 

### Fields:

**Description**: * Used by {@link Configuration}s to provide some value.
- **Extends**: ValueProvider

## 

### Fields:

- **Extends**: WayangException
- **warningSlf4jFormat**: `String` (private)
- **provideFor()**: Returns `Value`
- **optionallyProvideFor()**: Returns `Optional<Value>`
- **provideLocally()**: Returns `Value`
- **setParent()**: Returns `void`
- **set()**: Returns `void`
- **getConfiguration()**: Returns `Configuration`

### Public Methods:

- **provideFor()**: Returns `Value`
- **optionallyProvideFor()**: Returns `Optional<Value>`
- **provideLocally()**: Returns `Value`
- **setParent()**: Returns `void`
- **set()**: Returns `void`
- **getConfiguration()**: Returns `Configuration`

## 

### Fields:

**Description**: * Implementation of {@link KeyValueProvider} that uses a {@link Map} to provide a value.
- **Extends**: KeyValueProvider
- **tryToProvide()**: Returns `Value`
- **set()**: Returns `void`

### Public Methods:

- **tryToProvide()**: Returns `Value`
- **set()**: Returns `void`

## 

### Fields:

- **Extends**: WayangException
- **warningSlf4j**: `String` (private)
- **provide()**: Returns `Value`
- **optionallyProvide()**: Returns `Optional<Value>`
- **withSlf4jWarning()**: Returns `ValueProvider<Value>`
- **getConfiguration()**: Returns `Configuration`

### Public Methods:

- **provide()**: Returns `Value`
- **optionallyProvide()**: Returns `Optional<Value>`
- **withSlf4jWarning()**: Returns `ValueProvider<Value>`
- **getConfiguration()**: Returns `Configuration`

## 

### Fields:

**Description**: * Exception that declares a problem of Wayang.
- **Extends**: RuntimeException

## 

### Fields:

- **Extends**: FunctionDescriptor
- **getInputType()**: Returns `DataUnitGroupType<InputType>`
- **getOutputType()**: Returns `BasicDataUnitType<OutputType>`
- **toString()**: Returns `String`

### Public Methods:

- **getInputType()**: Returns `DataUnitGroupType<InputType>`
- **getOutputType()**: Returns `BasicDataUnitType<OutputType>`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * Created by bertty on 13-07-17.
- **Extends**: FunctionDescriptor
- **selectivity**: `ProbabilisticDoubleInterval` (private)
- **getJavaImplementation()**: Returns `SerializableConsumer<T>`
- **unchecked()**: Returns `SerializableConsumer<Object>`
- **getInputType()**: Returns `BasicDataUnitType<T>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

### Public Methods:

- **getJavaImplementation()**: Returns `SerializableConsumer<T>`
- **unchecked()**: Returns `SerializableConsumer<Object>`
- **getInputType()**: Returns `BasicDataUnitType<T>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

## 

## 

## 

### Fields:

**Description**: * This descriptor pertains to functions that consume a single data unit and output a group of data units.
- **Extends**: FunctionDescriptor
- **selectivity**: `ProbabilisticDoubleInterval` (private)
- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

### Public Methods:

- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: Function
- **loadProfileEstimator**: `LoadProfileEstimator` (private)
- **setLoadProfileEstimator()**: Returns `void`
- **getLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **setLoadEstimators()**: Returns `void`

### Public Methods:

- **setLoadProfileEstimator()**: Returns `void`
- **getLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **setLoadEstimators()**: Returns `void`

## 

### Fields:

**Description**: * This descriptor pertains to functions that consume and output multiple data quanta.
- **Extends**: FunctionDescriptor
- **selectivity**: `ProbabilisticDoubleInterval` (private)
- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

### Public Methods:

- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * This descriptor pertains to predicates that consume a single data unit.
- **Extends**: FunctionDescriptor
- **sqlImplementation**: `String` (private)
- **selectivity**: `ProbabilisticDoubleInterval` (private)
- **getJavaImplementation()**: Returns `SerializablePredicate<Input>`
- **getSqlImplementation()**: Returns `String`
- **withSqlImplementation()**: Returns `PredicateDescriptor<Input>`
- **unchecked()**: Returns `PredicateDescriptor<Object>`
- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

### Public Methods:

- **getJavaImplementation()**: Returns `SerializablePredicate<Input>`
- **getSqlImplementation()**: Returns `String`
- **withSqlImplementation()**: Returns `PredicateDescriptor<Input>`
- **unchecked()**: Returns `PredicateDescriptor<Object>`
- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getSelectivity()**: Returns `Optional<ProbabilisticDoubleInterval>`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * This descriptor pertains to functions that take multiple data units and aggregate them into a single data unit
- **Extends**: FunctionDescriptor
- **getJavaImplementation()**: Returns `BinaryOperator<Type>`
- **unchecked()**: Returns `ReduceDescriptor<Object>`
- **getInputType()**: Returns `DataUnitGroupType<Type>`
- **getOutputType()**: Returns `BasicDataUnitType<Type>`
- **toString()**: Returns `String`

### Public Methods:

- **getJavaImplementation()**: Returns `BinaryOperator<Type>`
- **unchecked()**: Returns `ReduceDescriptor<Object>`
- **getInputType()**: Returns `DataUnitGroupType<Type>`
- **getOutputType()**: Returns `BasicDataUnitType<Type>`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * This descriptor pertains to functions that consume a single data unit and output a single data unit.
- **Extends**: FunctionDescriptor
- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **toString()**: Returns `String`

### Public Methods:

- **getInputType()**: Returns `BasicDataUnitType<Input>`
- **getOutputType()**: Returns `BasicDataUnitType<Output>`
- **toString()**: Returns `String`

## 

## 

### Fields:

**Description**: * An operator match correlates an {@link OperatorPattern} to an actually matched {@link Operator}.
- **getPattern()**: Returns `OperatorPattern`
- **getOperator()**: Returns `Operator`

### Public Methods:

- **getPattern()**: Returns `OperatorPattern`
- **getOperator()**: Returns `Operator`

## 

### Fields:

**Description**: * An operator pattern matches to a class of operator instances.
- **Extends**: Operator
- **match()**: Returns `OperatorMatch`
- **withAdditionalTest()**: Returns `OperatorPattern<T>`
- **getName()**: Returns `String`
- **toString()**: Returns `String`
- **matchOperatorClass()**: Returns `boolean`
- **matchSlots()**: Returns `boolean`
- **matchSlot()**: Returns `boolean`
- **matchAdditionalTests()**: Returns `boolean`
- **checkSanity()**: Returns `void`

### Public Methods:

- **match()**: Returns `OperatorMatch`
- **withAdditionalTest()**: Returns `OperatorPattern<T>`
- **getName()**: Returns `String`
- **toString()**: Returns `String`
- **matchOperatorClass()**: Returns `boolean`
- **matchSlots()**: Returns `boolean`
- **matchSlot()**: Returns `boolean`
- **matchAdditionalTests()**: Returns `boolean`
- **checkSanity()**: Returns `void`

## 

### Fields:

**Description**: * Looks for a {@link SubplanPattern} in a {@link WayangPlan} and replaces it with an alternative {@link Operator}s.
- **thatReplaces()**: Returns `PlanTransformation`
- **transform()**: Returns `int`
- **getTargetPlatforms()**: Returns `Collection<Platform>`
- **meetsPlatformRestrictions()**: Returns `boolean`
- **introduceAlternative()**: Returns `void`
- **replace()**: Returns `void`

### Public Methods:

- **thatReplaces()**: Returns `PlanTransformation`
- **transform()**: Returns `int`
- **getTargetPlatforms()**: Returns `Collection<Platform>`
- **meetsPlatformRestrictions()**: Returns `boolean`
- **introduceAlternative()**: Returns `void`
- **replace()**: Returns `void`

## 

### Fields:

- **Extends**: Operator
- **createReplacementSubplan()**: Returns `Operator`
- **setNameTo()**: Returns `void`

### Public Methods:

- **createReplacementSubplan()**: Returns `Operator`
- **setNameTo()**: Returns `void`

## 

### Fields:

**Description**: * A subplan match correlates a {@link SubplanPattern} with its actually matched .
- **addOperatorMatch()**: Returns `void`
- **getPattern()**: Returns `SubplanPattern`
- **getInputMatch()**: Returns `OperatorMatch`
- **getOutputMatch()**: Returns `OperatorMatch`
- **getMatch()**: Returns `OperatorMatch`
- **getMaximumEpoch()**: Returns `int`
- **getTargetPlatforms()**: Returns `Optional<Set<Platform>>`

### Public Methods:

- **addOperatorMatch()**: Returns `void`
- **getPattern()**: Returns `SubplanPattern`
- **getInputMatch()**: Returns `OperatorMatch`
- **getOutputMatch()**: Returns `OperatorMatch`
- **getMatch()**: Returns `OperatorMatch`
- **getMaximumEpoch()**: Returns `int`
- **getTargetPlatforms()**: Returns `Optional<Set<Platform>>`

## 

### Fields:

**Description**: * A subplan pattern describes a class of subplans in a {@link WayangPlan}.
- **Extends**: OperatorBase
- **match()**: Returns `List<SubplanMatch>`
- **getInputPattern()**: Returns `OperatorPattern`
- **getOutputPattern()**: Returns `OperatorPattern`
- **match()**: Returns `List<SubplanMatch>`
- **attemptMatchFrom()**: Returns `void`
- **match()**: Returns `void`

### Public Methods:

- **match()**: Returns `List<SubplanMatch>`
- **getInputPattern()**: Returns `OperatorPattern`
- **getOutputPattern()**: Returns `OperatorPattern`
- **match()**: Returns `List<SubplanMatch>`
- **attemptMatchFrom()**: Returns `void`
- **match()**: Returns `void`

## 

### Fields:

- **Extends**: Monitor
- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

## 

### Fields:

- **Extends**: Monitor
- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

## 

### Fields:

**Description**: * TODO: Implement
- **Extends**: Monitor
- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

## 

## 

### Fields:

**Description**: * TODO: Implement
- **Extends**: Monitor
- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

### Public Methods:

- **initialize()**: Returns `void`
- **updateProgress()**: Returns `void`

## 

### Fields:

**Description**: * This {@link OptimizationContext} implementation aggregates several {@link OptimizationContext}s and exposes
- **Extends**: OptimizationContext
- **addOneTimeOperator()**: Returns `OperatorContext`
- **addOneTimeLoop()**: Returns `void`
- **getOperatorContext()**: Returns `OperatorContext`
- **updateOperatorContexts()**: Returns `void`
- **getNestedLoopContext()**: Returns `LoopContext`
- **clearMarks()**: Returns `void`
- **isTimeEstimatesComplete()**: Returns `boolean`
- **mergeToBase()**: Returns `void`
- **getDefaultOptimizationContexts()**: Returns `List<DefaultOptimizationContext>`
- **aggregateOperatorContext()**: Returns `OperatorContext`
- **updateOperatorContext()**: Returns `void`

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
- **aggregateOperatorContext()**: Returns `OperatorContext`
- **updateOperatorContext()**: Returns `void`

## 

### Fields:

**Description**: * This implementation of {@link OptimizationContext} represents a direct mapping from {@link OptimizationContext.OperatorContext}
- **Extends**: OptimizationContext
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

## 

### Fields:

**Description**: * Manages contextual information required during the optimization of a {@link WayangPlan}.
- **Implements**: EstimationContext
- **iterationNumber**: `int` (private)
- **loadProfile**: `LoadProfile` (private)
- **costEstimate**: `ProbabilisticDoubleInterval` (private)
- **squashedCostEstimate**: `double` (private)
- **lineage**: `ExecutionLineageNode` (private)
- **aggregateOptimizationContext**: `AggregateOptimizationContext` (private)
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
- **updateCostEstimate()**: Returns `void`
- **addTo()**: Returns `void`
- **addTo()**: Returns `void`

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
- **updateCostEstimate()**: Returns `void`
- **addTo()**: Returns `void`
- **addTo()**: Returns `void`

## 

### Fields:

**Description**: * Utility methods for the optimization process.
- **Extends**: PlanEnumerationPruningStrategy

## 

### Fields:

**Description**: *
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

## 

### Fields:

**Description**: *
- **Extends**: ProbabilisticIntervalEstimate
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

## 

### Fields:

**Description**: * This class checks a {@link WayangPlan} for several sanity criteria:
- **checkAllCriteria()**: Returns `boolean`
- **checkProperSubplans()**: Returns `boolean`
- **checkFlatAlternatives()**: Returns `boolean`
- **checkSubplanNotASingleton()**: Returns `void`
- **traverse()**: Returns `PlanTraversal`

### Public Methods:

- **checkAllCriteria()**: Returns `boolean`
- **checkProperSubplans()**: Returns `boolean`
- **checkFlatAlternatives()**: Returns `boolean`
- **checkSubplanNotASingleton()**: Returns `void`
- **traverse()**: Returns `PlanTraversal`

## 

### Fields:

- **Extends**: CardinalityPusher

## 

### Fields:

**Description**: * {@link CardinalityEstimator} implementation that can have multiple ways of calculating a {@link CardinalityEstimate}.
- **Implements**: CardinalityEstimator
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * An estimate of cardinality within a {@link WayangPlan} expressed as a {@link ProbabilisticIntervalEstimate}.
- **Extends**: ProbabilisticIntervalEstimate
- **Implements**: JsonSerializable
- **plus()**: Returns `CardinalityEstimate`
- **divideBy()**: Returns `CardinalityEstimate`
- **toJson()**: Returns `WayangJsonObj`
- **toJson()**: Returns `WayangJsonObj`
- **toString()**: Returns `String`

### Public Methods:

- **plus()**: Returns `CardinalityEstimate`
- **divideBy()**: Returns `CardinalityEstimate`
- **toJson()**: Returns `WayangJsonObj`
- **toJson()**: Returns `WayangJsonObj`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * {@link CardinalityEstimator} that subsumes a DAG of operators, each one providing a local {@link CardinalityEstimator}.
- **Extends**: Activator
- **result**: `CardinalityEstimationTraversal` (private)
- **traverse()**: Returns `boolean`
- **toString()**: Returns `String`
- **getTargetActivator()**: Returns `Activator`
- **fire()**: Returns `void`
- **doExecute()**: Returns `void`
- **initializeActivatorQueue()**: Returns `Queue<Activator>`
- **reset()**: Returns `void`
- **resetAll()**: Returns `void`
- **resetDownstream()**: Returns `void`
- **processDependentActivations()**: Returns `void`
- **addAndRegisterActivator()**: Returns `void`

### Public Methods:

- **traverse()**: Returns `boolean`
- **toString()**: Returns `String`
- **getTargetActivator()**: Returns `Activator`
- **fire()**: Returns `void`
- **doExecute()**: Returns `void`
- **initializeActivatorQueue()**: Returns `Queue<Activator>`
- **reset()**: Returns `void`
- **resetAll()**: Returns `void`
- **resetDownstream()**: Returns `void`
- **processDependentActivations()**: Returns `void`
- **addAndRegisterActivator()**: Returns `void`

## 

## 

### Fields:

**Description**: * Handles the {@link CardinalityEstimate}s of a {@link WayangPlan}.
- **planTraversal**: `CardinalityEstimationTraversal` (private)
- **pushCardinalities()**: Returns `boolean`
- **pushCardinalities()**: Returns `boolean`
- **updateConversionOperatorCardinalities()**: Returns `void`
- **getPlanTraversal()**: Returns `CardinalityEstimationTraversal`
- **pushCardinalityUpdates()**: Returns `boolean`
- **injectMeasuredCardinalities()**: Returns `boolean`
- **injectMeasuredCardinality()**: Returns `void`
- **injectMeasuredCardinality()**: Returns `void`

### Public Methods:

- **pushCardinalities()**: Returns `boolean`
- **pushCardinalities()**: Returns `boolean`
- **updateConversionOperatorCardinalities()**: Returns `void`
- **getPlanTraversal()**: Returns `CardinalityEstimationTraversal`
- **pushCardinalityUpdates()**: Returns `boolean`
- **injectMeasuredCardinalities()**: Returns `boolean`
- **injectMeasuredCardinality()**: Returns `void`
- **injectMeasuredCardinality()**: Returns `void`

## 

### Fields:

- **push()**: Returns `boolean`

### Public Methods:

- **push()**: Returns `boolean`

## 

### Fields:

**Description**: * Default implementation of the {@link CardinalityEstimator}. Generalizes a single-point estimation function.
- **Implements**: CardinalityEstimator
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * Default {@link CardinalityPusher} implementation. Bundles all {@link CardinalityEstimator}s of an {@link Operator}.
- **Extends**: CardinalityPusher

## 

### Fields:

**Description**: * Assumes with a confidence of 50% that the output cardinality will be somewhere between 1 and the product of
- **Implements**: CardinalityEstimator
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * {@link CardinalityEstimator} implementation for {@link Operator}s with a fix-sized output.
- **Implements**: CardinalityEstimator
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * {@link CardinalityPusher} implementation for {@link LoopHeadAlternative}s.
- **Extends**: AbstractAlternativeCardinalityPusher
- **pushThroughAlternatives()**: Returns `void`

### Public Methods:

- **pushThroughAlternatives()**: Returns `void`

## 

### Fields:

**Description**: * {@link CardinalityPusher} implementation for {@link LoopSubplan}s.
- **Extends**: CardinalityPusher

## 

### Fields:

**Description**: * {@link CardinalityPusher} implementation for {@link OperatorAlternative}s.
- **Extends**: AbstractAlternativeCardinalityPusher
- **pushThroughAlternatives()**: Returns `void`
- **pushThroughPath()**: Returns `void`

### Public Methods:

- **pushThroughAlternatives()**: Returns `void`
- **pushThroughPath()**: Returns `void`

## 

### Fields:

**Description**: * {@link CardinalityPusher} implementation for {@link Subplan}s (but not for {@link LoopSubplan}s!)
- **Extends**: CardinalityPusher

## 

### Fields:

**Description**: * Forwards the {@link CardinalityEstimate} of any given {@link InputSlot} that is not {@code null}. Asserts that
- **Implements**: CardinalityEstimator
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

- **convert()**: Returns `Channel`
- **getSourceChannelDescriptor()**: Returns `ChannelDescriptor`
- **getTargetChannelDescriptor()**: Returns `ChannelDescriptor`
- **toString()**: Returns `String`

### Public Methods:

- **convert()**: Returns `Channel`
- **getSourceChannelDescriptor()**: Returns `ChannelDescriptor`
- **getTargetChannelDescriptor()**: Returns `ChannelDescriptor`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * This graph contains a set of {@link ChannelConversion}s.
- **Extends**: OneTimeExecutable
- **Implements**: TreeSelectionStrategy
- **root**: `TreeVertex` (private)
- **add()**: Returns `void`
- **findMinimumCostJunction()**: Returns `Junction`
- **findMinimumCostJunction()**: Returns `Junction`
- **select()**: Returns `Tree`
- **select()**: Returns `Tree`
- **getJunction()**: Returns `Junction`
- **toString()**: Returns `String`
- **toString()**: Returns `String`
- **toString()**: Returns `String`
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

## 

### Fields:

**Description**: * Default implementation of the {@link ChannelConversion}. Can be used without further subclassing.
- **Extends**: ChannelConversion
- **convert()**: Returns `Channel`
- **update()**: Returns `void`
- **estimateConversionCost()**: Returns `ProbabilisticDoubleInterval`
- **isFiltered()**: Returns `boolean`
- **toString()**: Returns `String`
- **setCardinalityAndTimeEstimates()**: Returns `void`
- **determineCardinality()**: Returns `CardinalityEstimate`
- **setCardinalityAndTimeEstimate()**: Returns `void`
- **setCardinality()**: Returns `void`

### Public Methods:

- **convert()**: Returns `Channel`
- **update()**: Returns `void`
- **estimateConversionCost()**: Returns `ProbabilisticDoubleInterval`
- **isFiltered()**: Returns `boolean`
- **toString()**: Returns `String`
- **setCardinalityAndTimeEstimates()**: Returns `void`
- **determineCardinality()**: Returns `CardinalityEstimate`
- **setCardinalityAndTimeEstimate()**: Returns `void`
- **setCardinality()**: Returns `void`

## 

### Fields:

**Description**: * {@link LoadProfileEstimator} that estimates a predefined {@link LoadProfile}.
- **Implements**: LoadProfileEstimator
- **nest()**: Returns `void`
- **estimate()**: Returns `LoadProfile`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **copy()**: Returns `LoadProfileEstimator`

### Public Methods:

- **nest()**: Returns `void`
- **estimate()**: Returns `LoadProfile`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **copy()**: Returns `LoadProfileEstimator`

## 

### Fields:

**Description**: * Implementation of {@link LoadEstimator} that uses a single-point cost function.
- **Extends**: LoadEstimator
- **calculate()**: Returns `LoadEstimate`

### Public Methods:

- **calculate()**: Returns `LoadEstimate`

## 

### Fields:

- **Extends**: EstimationContext
- **getDoubleProperty()**: Returns `double`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getNumExecutions()**: Returns `int`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `EstimationContext`

### Public Methods:

- **getDoubleProperty()**: Returns `double`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getNumExecutions()**: Returns `int`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `EstimationContext`

## 

### Fields:

**Description**: * Implementation of {@link LoadEstimator} that uses a interval-based cost function.
- **Extends**: LoadEstimator
- **calculate()**: Returns `LoadEstimate`

### Public Methods:

- **calculate()**: Returns `LoadEstimate`

## 

### Fields:

**Description**: * An estimate of costs of some executable code expressed as a {@link ProbabilisticIntervalEstimate}.
- **Extends**: ProbabilisticIntervalEstimate
- **Implements**: JsonSerializable
- **times()**: Returns `LoadEstimate`
- **plus()**: Returns `LoadEstimate`
- **toJson()**: Returns `WayangJsonObj`

### Public Methods:

- **times()**: Returns `LoadEstimate`
- **plus()**: Returns `LoadEstimate`
- **toJson()**: Returns `WayangJsonObj`

## 

### Fields:

- **calculateJointProbability()**: Returns `double`

### Public Methods:

- **calculateJointProbability()**: Returns `double`

## 

### Fields:

**Description**: * Reflects the (estimated) required resources of an {@link Operator} or {@link FunctionDescriptor}.
- **Implements**: JsonSerializable
- **overheadMillis**: `long` (private)
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

## 

## 

### Fields:

**Description**: * Utilities to deal with {@link LoadProfileEstimator}s.
- **Extends**: ExecutionOperator
- **getVariable()**: Returns `double`

### Public Methods:

- **getVariable()**: Returns `double`

## 

### Fields:

- **convert()**: Returns `TimeEstimate`
- **sumWithSubprofiles()**: Returns `TimeEstimate`

### Public Methods:

- **convert()**: Returns `TimeEstimate`
- **sumWithSubprofiles()**: Returns `TimeEstimate`

## 

### Fields:

- **convert()**: Returns `TimeEstimate`
- **toString()**: Returns `String`

### Public Methods:

- **convert()**: Returns `TimeEstimate`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
- **Implements**: LoadProfileEstimator
- **nest()**: Returns `void`
- **estimate()**: Returns `LoadProfile`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **copy()**: Returns `LoadProfileEstimator`
- **toString()**: Returns `String`
- **performLocalEstimation()**: Returns `LoadProfile`
- **estimateResourceUtilization()**: Returns `double`
- **getOverheadMillis()**: Returns `long`

### Public Methods:

- **nest()**: Returns `void`
- **estimate()**: Returns `LoadProfile`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **copy()**: Returns `LoadProfileEstimator`
- **toString()**: Returns `String`
- **performLocalEstimation()**: Returns `LoadProfile`
- **estimateResourceUtilization()**: Returns `double`
- **getOverheadMillis()**: Returns `long`

## 

### Fields:

**Description**: * This {@link EstimationContext} implementation just stores all required variables without any further logic.
- **Extends**: SimpleEstimationContext
- **Implements**: EstimationContext
- **getDoubleProperty()**: Returns `double`
- **getNumExecutions()**: Returns `int`
- **getPropertyKeys()**: Returns `Collection<String>`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `SimpleEstimationContext`
- **deserialize()**: Returns `SimpleEstimationContext`

### Public Methods:

- **getDoubleProperty()**: Returns `double`
- **getNumExecutions()**: Returns `int`
- **getPropertyKeys()**: Returns `Collection<String>`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `SimpleEstimationContext`
- **deserialize()**: Returns `SimpleEstimationContext`

## 

### Fields:

**Description**: * An estimate of time (in <b>milliseconds</b>) expressed as a {@link ProbabilisticIntervalEstimate}.
- **Extends**: ProbabilisticIntervalEstimate
- **plus()**: Returns `TimeEstimate`
- **plus()**: Returns `TimeEstimate`
- **times()**: Returns `TimeEstimate`
- **toString()**: Returns `String`
- **toIntervalString()**: Returns `String`
- **toGMeanString()**: Returns `String`

### Public Methods:

- **plus()**: Returns `TimeEstimate`
- **plus()**: Returns `TimeEstimate`
- **times()**: Returns `TimeEstimate`
- **toString()**: Returns `String`
- **toIntervalString()**: Returns `String`
- **toGMeanString()**: Returns `String`

## 

### Fields:

**Description**: * This (linear) converter turns {@link TimeEstimate}s into cost estimates.
- **convert()**: Returns `ProbabilisticDoubleInterval`
- **convertWithoutFixCosts()**: Returns `ProbabilisticDoubleInterval`
- **getFixCosts()**: Returns `double`
- **getCostsPerMillisecond()**: Returns `double`

### Public Methods:

- **convert()**: Returns `ProbabilisticDoubleInterval`
- **convertWithoutFixCosts()**: Returns `ProbabilisticDoubleInterval`
- **getFixCosts()**: Returns `double`
- **getCostsPerMillisecond()**: Returns `double`

## 

### Fields:

**Description**: * Graph of {@link ExecutionTask}s and {@link Channel}s. Does not define {@link ExecutionStage}s and
- **collectAllTasks()**: Returns `Set<ExecutionTask>`
- **isComplete()**: Returns `boolean`
- **getSinkTasks()**: Returns `Collection<ExecutionTask>`
- **collectAllTasksAux()**: Returns `void`
- **collectAllTasksAux()**: Returns `void`

### Public Methods:

- **collectAllTasks()**: Returns `Set<ExecutionTask>`
- **isComplete()**: Returns `boolean`
- **getSinkTasks()**: Returns `Collection<ExecutionTask>`
- **collectAllTasksAux()**: Returns `void`
- **collectAllTasksAux()**: Returns `void`

## 

### Fields:

**Description**: * Creates an {@link ExecutionTaskFlow} from a {@link PlanImplementation}.
- **Extends**: AbstractTopologicalTraversal
- **executionTask**: `ExecutionTask` (private)
- **getTerminalTasks()**: Returns `Collection<ExecutionTask>`
- **getInputChannels()**: Returns `Set<Channel>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **getOrCreateExecutionTask()**: Returns `ExecutionTask`
- **connectToSuccessorTasks()**: Returns `void`
- **getJunction()**: Returns `Junction`
- **createActivation()**: Returns `void`

### Public Methods:

- **getTerminalTasks()**: Returns `Collection<ExecutionTask>`
- **getInputChannels()**: Returns `Set<Channel>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **getOrCreateExecutionTask()**: Returns `ExecutionTask`
- **connectToSuccessorTasks()**: Returns `void`
- **getJunction()**: Returns `Junction`
- **createActivation()**: Returns `void`

## 

### Fields:

**Description**: * This {@link PlanEnumerationPruningStrategy} follows the idea that we can prune a
- **Implements**: PlanEnumerationPruningStrategy
- **configure()**: Returns `void`
- **prune()**: Returns `void`
- **selectBestPlanNary()**: Returns `PlanImplementation`
- **selectBestPlanBinary()**: Returns `PlanImplementation`

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`
- **selectBestPlanNary()**: Returns `PlanImplementation`
- **selectBestPlanBinary()**: Returns `PlanImplementation`

## 

### Fields:

**Description**: * Enumerator for {@link LoopSubplan}s.
- **Extends**: OneTimeExecutable
- **loopEnumeration**: `PlanEnumeration` (private)
- **enumerate()**: Returns `PlanEnumeration`
- **addFeedbackConnections()**: Returns `void`
- **addFeedbackConnection()**: Returns `boolean`

### Public Methods:

- **enumerate()**: Returns `PlanEnumeration`
- **addFeedbackConnections()**: Returns `void`
- **addFeedbackConnection()**: Returns `boolean`

## 

### Fields:

**Description**: * Describes the enumeration of a {@link LoopSubplan}.
- **Implements**: the given
- **interBodyJunction**: `Junction` (private)
- **forwardJunction**: `Junction` (private)
- **enterJunction**: `Junction` (private)
- **exitJunction**: `Junction` (private)
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

## 

### Fields:

**Description**: * Represents a collection of {@link PlanImplementation}s that all implement the same section of a {@link WayangPlan} (which
- **concatenate()**: Returns `PlanEnumeration`
- **add()**: Returns `void`
- **unionInPlace()**: Returns `void`
- **escape()**: Returns `PlanEnumeration`
- **getPlanImplementations()**: Returns `Collection<PlanImplementation>`
- **getScope()**: Returns `Set<OperatorAlternative>`
- **toString()**: Returns `String`
- **concatenatePartialPlans()**: Returns `Collection<PlanImplementation>`
- **concatenatePartialPlansBatchwise()**: Returns `Collection<PlanImplementation>`
- **createSingletonPartialPlan()**: Returns `PlanImplementation`
- **toIOString()**: Returns `String`
- **toScopeString()**: Returns `String`

### Public Methods:

- **concatenate()**: Returns `PlanEnumeration`
- **add()**: Returns `void`
- **unionInPlace()**: Returns `void`
- **escape()**: Returns `PlanEnumeration`
- **getPlanImplementations()**: Returns `Collection<PlanImplementation>`
- **getScope()**: Returns `Set<OperatorAlternative>`
- **toString()**: Returns `String`
- **concatenatePartialPlans()**: Returns `Collection<PlanImplementation>`
- **concatenatePartialPlansBatchwise()**: Returns `Collection<PlanImplementation>`
- **createSingletonPartialPlan()**: Returns `PlanImplementation`
- **toIOString()**: Returns `String`
- **toScopeString()**: Returns `String`

## 

## 

### Fields:

**Description**: * The plan partitioner recursively dissects a {@link WayangPlan} into {@link PlanEnumeration}s and then assembles
- **resultReference**: `AtomicReference<PlanEnumeration>` (private)
- **timeMeasurement**: `TimeMeasurement` (private)
- **isEnumeratingBranchesFirst**: `boolean` (private)
- **baseEnumeration**: `PlanEnumeration` (private)
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

## 

### Fields:

**Description**: * Represents a partial execution plan.
- **planEnumeration**: `PlanEnumeration` (private)
- **platformCache**: `Set<Platform>` (private)
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
- **copyLoopImplementations()**: Returns `PlanImplementation`
- **isSettledAlternativesContradicting()**: Returns `boolean`
- **allOutermostOutputSlots()**: Returns `Stream<OutputSlot>`
- **allOutermostInputSlots()**: Returns `Stream<InputSlot>`
- **isStartOperator()**: Returns `boolean`

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
- **copyLoopImplementations()**: Returns `PlanImplementation`
- **isSettledAlternativesContradicting()**: Returns `boolean`
- **allOutermostOutputSlots()**: Returns `Stream<OutputSlot>`
- **allOutermostInputSlots()**: Returns `Stream<InputSlot>`
- **isStartOperator()**: Returns `boolean`

## 

### Fields:

- **Implements**: PlanEnumerationPruningStrategy
- **random**: `Random` (private)
- **numRetainedPlans**: `int` (private)
- **configure()**: Returns `void`
- **prune()**: Returns `void`

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`

## 

### Fields:

- **Implements**: PlanEnumerationPruningStrategy
- **configure()**: Returns `void`
- **prune()**: Returns `void`

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`

## 

### Fields:

**Description**: * Builds an {@link ExecutionPlan} from a {@link ExecutionTaskFlow}.
- **Extends**: OneTimeExecutable
- **Implements**: InterimStage
- **result**: `ExecutionPlan` (private)
- **isMarked**: `boolean` (private)
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

## 

### Fields:

**Description**: * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.
- **Implements**: PlanEnumerationPruningStrategy
- **k**: `int` (private)
- **configure()**: Returns `void`
- **prune()**: Returns `void`
- **comparePlanImplementations()**: Returns `int`

### Public Methods:

- **configure()**: Returns `void`
- **prune()**: Returns `void`
- **comparePlanImplementations()**: Returns `int`

## 

### Fields:

**Description**: * An enumeration alternative is embedded within an {@link EnumerationBranch}.

## 

### Fields:

**Description**: * An enumeration branch is the basic unit for enumeration, i.e., translation from a {@link WayangPlan} to an

## 

### Fields:

- **Extends**: OutputSlot
- **producer**: `ExecutionTask` (private)
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
- **withSiblings()**: Returns `Stream<Channel>`
- **relateTo()**: Returns `void`
- **copyConsumersFrom()**: Returns `void`
- **adoptSiblings()**: Returns `void`

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
- **withSiblings()**: Returns `Stream<Channel>`
- **relateTo()**: Returns `void`
- **copyConsumersFrom()**: Returns `void`
- **adoptSiblings()**: Returns `void`

## 

## 

### Fields:

**Description**: * Represents an executable, cross-platform data flow. Consists of muliple {@link PlatformExecution}s.
- **Extends**: this
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

## 

### Fields:

**Description**: * Resides within a {@link PlatformExecution} and represents the minimum execution unit that is controlled by Wayang.
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
- **updateLoop()**: Returns `void`
- **toExtensiveStringAux()**: Returns `void`
- **toJsonMapAux()**: Returns `void`
- **prettyPrint()**: Returns `String`
- **prettyPrint()**: Returns `String`

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
- **updateLoop()**: Returns `void`
- **toExtensiveStringAux()**: Returns `void`
- **toJsonMapAux()**: Returns `void`
- **prettyPrint()**: Returns `String`
- **prettyPrint()**: Returns `String`

## 

### Fields:

**Description**: * This class models the execution equivalent of {@link LoopSubplan}s.
- **headStageCache**: `ExecutionStage` (private)
- **add()**: Returns `void`
- **update()**: Returns `void`
- **getLoopHead()**: Returns `ExecutionStage`
- **getLoopSubplan()**: Returns `LoopSubplan`
- **checkForLoopHead()**: Returns `boolean`
- **isLoopHead()**: Returns `boolean`

### Public Methods:

- **add()**: Returns `void`
- **update()**: Returns `void`
- **getLoopHead()**: Returns `ExecutionStage`
- **getLoopSubplan()**: Returns `LoopSubplan`
- **checkForLoopHead()**: Returns `boolean`
- **isLoopHead()**: Returns `boolean`

## 

### Fields:

**Description**: * Serves as an adapter to include {@link ExecutionOperator}s, which are usually parts of {@link WayangPlan}s, in
- **Implements**: a feedback
- **stage**: `ExecutionStage` (private)
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

## 

### Fields:

**Description**: * Complete data flow on a single platform, that consists of multiple {@link ExecutionStage}s.
- **getStages()**: Returns `Collection<ExecutionStage>`
- **getPlatform()**: Returns `Platform`
- **createStage()**: Returns `ExecutionStage`
- **toString()**: Returns `String`
- **retain()**: Returns `void`

### Public Methods:

- **getStages()**: Returns `Collection<ExecutionStage>`
- **getPlatform()**: Returns `Platform`
- **createStage()**: Returns `ExecutionStage`
- **toString()**: Returns `String`
- **retain()**: Returns `void`

## 

### Fields:

- **Extends**: Operator

## 

### Fields:

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator
- **getInputType0()**: Returns `DataSetType<InputType0>`
- **getInputType1()**: Returns `DataSetType<InputType1>`
- **getOutputType()**: Returns `DataSetType<OutputType>`

### Public Methods:

- **getInputType0()**: Returns `DataSetType<InputType0>`
- **getInputType1()**: Returns `DataSetType<InputType1>`
- **getOutputType()**: Returns `DataSetType<OutputType>`

## 

### Fields:

- **Extends**: Operator

## 

### Fields:

- **Extends**: ActualOperator

## 

## 

### Fields:

- **Extends**: ElementaryOperator

## 

### Fields:

**Description**: * An input slot declares an input of an {@link Operator}.
- **Extends**: Slot
- **occupant**: `OutputSlot<T>` (private)
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

## 

### Fields:

**Description**: * Special {@link OperatorAlternative} for {@link LoopHeadOperator}s.
- **Extends**: OperatorAlternative
- **Implements**: LoopHeadOperator
- **addAlternative()**: Returns `Alternative`
- **getNumExpectedIterations()**: Returns `int`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **getInitializationPusher()**: Returns `CardinalityPusher`
- **getFinalizationPusher()**: Returns `CardinalityPusher`

### Public Methods:

- **addAlternative()**: Returns `Alternative`
- **getNumExpectedIterations()**: Returns `int`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **getInitializationPusher()**: Returns `CardinalityPusher`
- **getFinalizationPusher()**: Returns `CardinalityPusher`

## 

### Fields:

- **Extends**: Operator

## 

### Fields:

**Description**: * Goes over a {@link WayangPlan} and isolates its loops.
- **Extends**: OneTimeExecutable
- **run()**: Returns `void`

### Public Methods:

- **run()**: Returns `void`

## 

### Fields:

**Description**: * Wraps a loop of {@link Operator}s.
- **Extends**: Subplan
- **loopHead**: `LoopHeadOperator` (private)
- **getNumExpectedIterations()**: Returns `int`
- **getLoopHead()**: Returns `LoopHeadOperator`
- **getInnerInputOptimizationContext()**: Returns `Collection<OptimizationContext>`
- **getInnerOutputOptimizationContext()**: Returns `OptimizationContext`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **noteReplaced()**: Returns `void`

### Public Methods:

- **getNumExpectedIterations()**: Returns `int`
- **getLoopHead()**: Returns `LoopHeadOperator`
- **getInnerInputOptimizationContext()**: Returns `Collection<OptimizationContext>`
- **getInnerOutputOptimizationContext()**: Returns `OptimizationContext`
- **getCardinalityPusher()**: Returns `CardinalityPusher`
- **noteReplaced()**: Returns `void`

## 

## 

### Fields:

**Description**: * This operator encapsulates operators that are alternative to each other.
- **Extends**: OperatorBase
- **Implements**: CompositeOperator
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

## 

### Fields:

- **Implements**: Operator
- **container**: `OperatorContainer` (private)
- **original**: `ExecutionOperator` (private)
- **name**: `String` (private)
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

## 

### Fields:

- **Extends**: InputSlot

## 

### Fields:

**Description**: * Utilities to deal with {@link OperatorContainer}s.

## 

### Fields:

**Description**: * Utility class for {@link Operator}s.

## 

### Fields:

**Description**: * An output slot declares an output of an {@link Operator}.
- **Extends**: Slot
- **stealOccupiedSlots()**: Returns `void`
- **getIndex()**: Returns `int`
- **copyFor()**: Returns `OutputSlot`
- **connectTo()**: Returns `void`
- **disconnectFrom()**: Returns `void`
- **getOccupiedSlots()**: Returns `List<InputSlot<T>>`
- **unchecked()**: Returns `OutputSlot<Object>`
- **collectRelatedSlots()**: Returns `Set<OutputSlot<T>>`
- **isFeedforward()**: Returns `boolean`

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

## 

### Fields:

- **Extends**: Measurement
- **numCombinations**: `long` (private)
- **getNumVirtualOperators()**: Returns `int`
- **getNumExecutionOperators()**: Returns `int`
- **getNumAlternatives()**: Returns `int`
- **getNumCombinations()**: Returns `long`
- **collectFrom()**: Returns `void`
- **collectFrom()**: Returns `long`
- **collectFrom()**: Returns `long`

### Public Methods:

- **getNumVirtualOperators()**: Returns `int`
- **getNumExecutionOperators()**: Returns `int`
- **getNumAlternatives()**: Returns `int`
- **getNumCombinations()**: Returns `long`
- **collectFrom()**: Returns `void`
- **collectFrom()**: Returns `long`
- **collectFrom()**: Returns `long`

## 

### Fields:

**Description**: * Traverse a plan. In each instance, every operator will be traversed only once.
- **Extends**: Operator
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
- **visit()**: Returns `boolean`
- **traverseHierarchical()**: Returns `boolean`
- **enter()**: Returns `void`
- **followOutputs()**: Returns `void`

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
- **visit()**: Returns `boolean`
- **traverseHierarchical()**: Returns `boolean`
- **enter()**: Returns `void`
- **followOutputs()**: Returns `void`

## 

### Fields:

- **Extends**: Slot
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

## 

### Fields:

**Description**: * This mapping can be used to encapsulate subplans by connecting slots (usually <b>against</b> the data flow direction,
- **mapAllUpsteam()**: Returns `void`
- **mapAllUpsteam()**: Returns `void`
- **mapUpstream()**: Returns `void`
- **mapUpstream()**: Returns `void`
- **replaceInputSlotMappings()**: Returns `void`
- **replaceOutputSlotMappings()**: Returns `void`
- **delete()**: Returns `void`
- **delete()**: Returns `void`

### Public Methods:

- **mapAllUpsteam()**: Returns `void`
- **mapAllUpsteam()**: Returns `void`
- **mapUpstream()**: Returns `void`
- **mapUpstream()**: Returns `void`
- **replaceInputSlotMappings()**: Returns `void`
- **replaceOutputSlotMappings()**: Returns `void`
- **delete()**: Returns `void`
- **delete()**: Returns `void`

## 

### Fields:

**Description**: * A subplan encapsulates connected operators as a single operator.
- **Extends**: OperatorBase
- **Implements**: ActualOperator, CompositeOperator, OperatorContainer
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

## 

### Fields:

- **process()**: Returns `Return`
- **visit()**: Returns `Return`

### Public Methods:

- **process()**: Returns `Return`
- **visit()**: Returns `Return`

## 

### Fields:

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator
- **getInput()**: Returns `InputSlot<T>`
- **getType()**: Returns `DataSetType<T>`

### Public Methods:

- **getInput()**: Returns `InputSlot<T>`
- **getType()**: Returns `DataSetType<T>`

## 

### Fields:

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator
- **getOutput()**: Returns `OutputSlot<T>`
- **getType()**: Returns `DataSetType<T>`

### Public Methods:

- **getOutput()**: Returns `OutputSlot<T>`
- **getType()**: Returns `DataSetType<T>`

## 

### Fields:

- **Extends**: OperatorBase
- **Implements**: ElementaryOperator
- **getInput()**: Returns `InputSlot<InputType>`
- **getOutput()**: Returns `OutputSlot<OutputType>`
- **getInputType()**: Returns `DataSetType<InputType>`
- **getOutputType()**: Returns `DataSetType<OutputType>`

### Public Methods:

- **getInput()**: Returns `InputSlot<InputType>`
- **getOutput()**: Returns `OutputSlot<OutputType>`
- **getInputType()**: Returns `DataSetType<InputType>`
- **getOutputType()**: Returns `DataSetType<OutputType>`

## 

### Fields:

**Description**: * A Wayang plan consists of a set of {@link Operator}s.
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
- **pruneUnreachableSuccessors()**: Returns `void`
- **applyAndCountTransformations()**: Returns `int`

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
- **pruneUnreachableSuccessors()**: Returns `void`
- **applyAndCountTransformations()**: Returns `int`

## 

### Fields:

- **Extends**: AbstractTopologicalTraversal
- **toString()**: Returns `String`

### Public Methods:

- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: ExecutionResourceTemplate
- **Implements**: ChannelInstance
- **lineage**: `ChannelLineageNode` (private)
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **setMeasuredCardinality()**: Returns `void`
- **getLineage()**: Returns `ChannelLineageNode`
- **wasProduced()**: Returns `boolean`
- **markProduced()**: Returns `void`
- **toString()**: Returns `String`

### Public Methods:

- **getMeasuredCardinality()**: Returns `OptionalLong`
- **setMeasuredCardinality()**: Returns `void`
- **getLineage()**: Returns `ChannelLineageNode`
- **wasProduced()**: Returns `boolean`
- **markProduced()**: Returns `void`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * An atomic execution describes the smallest work unit considered by Wayang's cost model.
- **Extends**: AtomicExecution
- **Implements**: JsonSerializer
- **loadProfileEstimator**: `LoadProfileEstimator` (private)
- **estimateLoad()**: Returns `LoadProfile`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `AtomicExecution`
- **deserialize()**: Returns `AtomicExecution`
- **getLoadProfileEstimator()**: Returns `LoadProfileEstimator`
- **setLoadProfileEstimator()**: Returns `void`
- **toString()**: Returns `String`
- **serialize()**: Returns `void`
- **deserializeEstimator()**: Returns `LoadProfileEstimator`

### Public Methods:

- **estimateLoad()**: Returns `LoadProfile`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `AtomicExecution`
- **deserialize()**: Returns `AtomicExecution`
- **getLoadProfileEstimator()**: Returns `LoadProfileEstimator`
- **setLoadProfileEstimator()**: Returns `void`
- **toString()**: Returns `String`
- **serialize()**: Returns `void`
- **deserializeEstimator()**: Returns `LoadProfileEstimator`

## 

### Fields:

**Description**: * This class groups {@link AtomicExecution}s with a common {@link EstimationContext} and {@link Platform}.
- **Extends**: AtomicExecutionGroup
- **Implements**: JsonSerializer
- **estimationContext**: `EstimationContext` (private)
- **platform**: `Platform` (private)
- **atomicExecutions**: `Collection<AtomicExecution>` (private)
- **configuration**: `Configuration` (private)
- **loadProfileToTimeConverterCache**: `LoadProfileToTimeConverter` (private)
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

## 

## 

### Fields:

**Description**: * {@link Breakpoint} implementation that is based on the {@link CardinalityEstimate}s of {@link Channel}s.
- **Implements**: Breakpoint
- **permitsExecutionOf()**: Returns `boolean`
- **approves()**: Returns `boolean`
- **calculateSpread()**: Returns `double`
- **getCardinalityEstimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **permitsExecutionOf()**: Returns `boolean`
- **approves()**: Returns `boolean`
- **calculateSpread()**: Returns `double`
- **getCardinalityEstimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * Describes a certain {@link Channel} type including further parameters.
- **Extends**: Channel
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **isReusable()**: Returns `boolean`
- **isSuitableForBreakpoint()**: Returns `boolean`
- **createChannel()**: Returns `Channel`

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **isReusable()**: Returns `boolean`
- **isSuitableForBreakpoint()**: Returns `boolean`
- **createChannel()**: Returns `Channel`

## 

### Fields:

- **Extends**: ExecutionResource

## 

### Fields:

- **Extends**: ExecutionResource

## 

### Fields:

**Description**: * {@link Breakpoint} implementation that disrupts execution if all aggregated {@link Breakpoint}s request a disruption.
- **Implements**: Breakpoint
- **permitsExecutionOf()**: Returns `boolean`
- **addConjunct()**: Returns `void`

### Public Methods:

- **permitsExecutionOf()**: Returns `boolean`
- **addConjunct()**: Returns `void`

## 

### Fields:

**Description**: * Executes a (cross-platform) {@link ExecutionPlan}.
- **Extends**: AbstractReferenceCountable
- **Implements**: ExecutionState
- **threadId**: `String` (public)
- **thread_isBreakpointDisabled**: `boolean` (private)
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

## 

### Fields:

- **Extends**: ReferenceCountable

## 

### Fields:

- **Extends**: AbstractReferenceCountable
- **Implements**: ExecutionResource
- **dispose()**: Returns `void`

### Public Methods:

- **dispose()**: Returns `void`

## 

## 

### Fields:

- **Extends**: CompositeExecutionResource

## 

### Fields:

- **Extends**: AbstractReferenceCountable
- **Implements**: Executor
- **register()**: Returns `void`
- **unregister()**: Returns `void`
- **dispose()**: Returns `void`
- **getCrossPlatformExecutor()**: Returns `CrossPlatformExecutor`
- **toString()**: Returns `String`
- **getConfiguration()**: Returns `Configuration`

### Public Methods:

- **register()**: Returns `void`
- **unregister()**: Returns `void`
- **dispose()**: Returns `void`
- **getCrossPlatformExecutor()**: Returns `CrossPlatformExecutor`
- **toString()**: Returns `String`
- **getConfiguration()**: Returns `Configuration`

## 

### Fields:

**Description**: * Describes when to interrupt the execution of an {@link ExecutionPlan}.
- **Implements**: Breakpoint
- **breakAfter()**: Returns `FixBreakpoint`
- **breakBefore()**: Returns `FixBreakpoint`
- **breakBefore()**: Returns `FixBreakpoint`
- **permitsExecutionOf()**: Returns `boolean`

### Public Methods:

- **breakAfter()**: Returns `FixBreakpoint`
- **breakBefore()**: Returns `FixBreakpoint`
- **breakBefore()**: Returns `FixBreakpoint`
- **permitsExecutionOf()**: Returns `boolean`

## 

### Fields:

**Description**: * Describes the implementation of one {@link OutputSlot} to its occupied {@link InputSlot}s.
- **sourceChannel**: `Channel` (private)
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
- **findMatchingOptimizationContext()**: Returns `OptimizationContext`

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
- **findMatchingOptimizationContext()**: Returns `OptimizationContext`

## 

### Fields:

**Description**: * This {@link Breakpoint} implementation always requests a break unless inside of {@link ExecutionStageLoop}s.
- **Implements**: Breakpoint
- **permitsExecutionOf()**: Returns `boolean`

### Public Methods:

- **permitsExecutionOf()**: Returns `boolean`

## 

### Fields:

**Description**: * Captures data of a execution of a set of {@link ExecutionOperator}s.
- **Extends**: PartialExecution
- **Implements**: JsonSerializer
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

## 

### Fields:

- **Extends**: Platform
- **createExecutor()**: Returns `Executor`
- **getName()**: Returns `String`
- **getConfigurationName()**: Returns `String`
- **toString()**: Returns `String`
- **isSinglePlatformExecutionPossible()**: Returns `boolean`
- **warmUp()**: Returns `void`
- **getInitializeMillis()**: Returns `long`
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `Platform`

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

## 

### Fields:

- **Extends**: ExecutorTemplate
- **execute()**: Returns `void`
- **accept()**: Returns `void`
- **getTask()**: Returns `ExecutionTask`
- **getJob()**: Returns `Job`
- **scheduleStartTask()**: Returns `void`
- **store()**: Returns `void`
- **activateSuccessorTasks()**: Returns `void`
- **executor()**: Returns `PushExecutorTemplate`
- **updateExecutionState()**: Returns `void`
- **acceptFrom()**: Returns `void`

### Public Methods:

- **execute()**: Returns `void`
- **accept()**: Returns `void`
- **getTask()**: Returns `ExecutionTask`
- **getJob()**: Returns `Job`
- **scheduleStartTask()**: Returns `void`
- **store()**: Returns `void`
- **activateSuccessorTasks()**: Returns `void`
- **executor()**: Returns `PushExecutorTemplate`
- **updateExecutionState()**: Returns `void`
- **acceptFrom()**: Returns `void`

## 

### Fields:

**Description**: * Encapsulates a {@link ChannelInstance} in the lazy execution lineage.
- **Extends**: LazyExecutionLineageNode
- **toString()**: Returns `String`
- **getChannelInstance()**: Returns `ChannelInstance`

### Public Methods:

- **toString()**: Returns `String`
- **getChannelInstance()**: Returns `ChannelInstance`

## 

### Fields:

**Description**: * Encapsulates {@link AtomicExecution}s with a common {@link OptimizationContext.OperatorContext} in a lazy execution lineage.
- **Extends**: LazyExecutionLineageNode
- **add()**: Returns `ExecutionLineageNode`
- **add()**: Returns `ExecutionLineageNode`
- **addAtomicExecutionFromOperatorContext()**: Returns `ExecutionLineageNode`
- **getAtomicExecutions()**: Returns `Collection<AtomicExecution>`
- **toString()**: Returns `String`

### Public Methods:

- **add()**: Returns `ExecutionLineageNode`
- **add()**: Returns `ExecutionLineageNode`
- **addAtomicExecutionFromOperatorContext()**: Returns `ExecutionLineageNode`
- **getAtomicExecutions()**: Returns `Collection<AtomicExecution>`
- **toString()**: Returns `String`

## 

### Fields:

- **Implements**: Aggregator
- **addPredecessor()**: Returns `void`

### Public Methods:

- **addPredecessor()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} can be arbitrarily customized.
- **Implements**: Plugin
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

## 

## 

### Fields:

**Description**: * Stores cardinalities that have been collected by the {@link CrossPlatformExecutor}. Current version uses
- **writer**: `BufferedWriter` (private)
- **storeAll()**: Returns `void`
- **store()**: Returns `void`
- **sleep()**: Returns `void`
- **write()**: Returns `void`
- **write()**: Returns `void`
- **getWriter()**: Returns `BufferedWriter`

### Public Methods:

- **storeAll()**: Returns `void`
- **store()**: Returns `void`
- **sleep()**: Returns `void`
- **write()**: Returns `void`
- **write()**: Returns `void`
- **getWriter()**: Returns `BufferedWriter`

## 

### Fields:

- **Extends**: Measurement
- **probability**: `double` (private)
- **getLowerCost()**: Returns `double`
- **setLowerCost()**: Returns `void`
- **getUpperCost()**: Returns `double`
- **setUpperCost()**: Returns `void`
- **getProbability()**: Returns `double`
- **setProbability()**: Returns `void`

### Public Methods:

- **getLowerCost()**: Returns `double`
- **setLowerCost()**: Returns `void`
- **getUpperCost()**: Returns `double`
- **setUpperCost()**: Returns `void`
- **getProbability()**: Returns `double`
- **setProbability()**: Returns `void`

## 

### Fields:

**Description**: * Stores execution data have been collected by the {@link CrossPlatformExecutor}.
- **Implements**: AutoCloseable
- **writer**: `BufferedWriter` (private)
- **storeAll()**: Returns `void`
- **store()**: Returns `void`
- **stream()**: Returns `Stream<PartialExecution>`
- **close()**: Returns `void`
- **store()**: Returns `void`
- **write()**: Returns `void`
- **getWriter()**: Returns `BufferedWriter`

### Public Methods:

- **storeAll()**: Returns `void`
- **store()**: Returns `void`
- **stream()**: Returns `Stream<PartialExecution>`
- **close()**: Returns `void`
- **store()**: Returns `void`
- **write()**: Returns `void`
- **getWriter()**: Returns `BufferedWriter`

## 

### Fields:

- **Extends**: Measurement
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

## 

### Fields:

**Description**: * Instruments only outbound {@link Channel}s.
- **Implements**: InstrumentationStrategy
- **applyTo()**: Returns `void`

### Public Methods:

- **applyTo()**: Returns `void`

## 

## 

### Fields:

- **Implements**: InstrumentationStrategy
- **applyTo()**: Returns `void`

### Public Methods:

- **applyTo()**: Returns `void`

## 

### Fields:

**Description**: * Instruments only outbound {@link Channel}s.
- **Implements**: InstrumentationStrategy
- **applyTo()**: Returns `void`

### Public Methods:

- **applyTo()**: Returns `void`

## 

### Fields:

- **Extends**: Measurement
- **executionMillis**: `long` (private)
- **estimatedExecutionMillis**: `TimeEstimate` (private)
- **getExecutionMillis()**: Returns `long`
- **setExecutionMillis()**: Returns `void`
- **getEstimatedExecutionMillis()**: Returns `TimeEstimate`
- **setEstimatedExecutionMillis()**: Returns `void`

### Public Methods:

- **getExecutionMillis()**: Returns `long`
- **setExecutionMillis()**: Returns `void`
- **getEstimatedExecutionMillis()**: Returns `TimeEstimate`
- **setEstimatedExecutionMillis()**: Returns `void`

## 

### Fields:

**Description**: * Utilities to work with {@link ProfileDB}s.

## 

### Fields:

**Description**: * A basic data unit type is elementary and not constructed from other data unit types.
- **Extends**: DataUnitType
- **isGroup()**: Returns `boolean`
- **toBasicDataUnitType()**: Returns `BasicDataUnitType<T>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getTypeClass()**: Returns `Class<T>`

### Public Methods:

- **isGroup()**: Returns `boolean`
- **toBasicDataUnitType()**: Returns `BasicDataUnitType<T>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getTypeClass()**: Returns `Class<T>`

## 

### Fields:

**Description**: * A data set is an abstraction of the Wayang programming model. Although never directly materialized, a data set
- **Extends**: T
- **getDataUnitType()**: Returns `DataUnitType<T>`
- **unchecked()**: Returns `DataSetType<Object>`
- **uncheckedGroup()**: Returns `DataSetType<Iterable<Object>>`
- **isSupertypeOf()**: Returns `boolean`
- **isNone()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

### Public Methods:

- **getDataUnitType()**: Returns `DataUnitType<T>`
- **unchecked()**: Returns `DataSetType<Object>`
- **uncheckedGroup()**: Returns `DataSetType<Iterable<Object>>`
- **isSupertypeOf()**: Returns `boolean`
- **isNone()**: Returns `boolean`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * A grouped data unit type describes just the structure of data units within a grouped dataset.
- **Extends**: DataUnitType
- **isGroup()**: Returns `boolean`
- **getTypeClass()**: Returns `Class<Iterable<T>>`
- **getBaseType()**: Returns `DataUnitType<T>`
- **toBasicDataUnitType()**: Returns `BasicDataUnitType<Iterable<T>>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

### Public Methods:

- **isGroup()**: Returns `boolean`
- **getTypeClass()**: Returns `Class<Iterable<T>>`
- **getBaseType()**: Returns `DataUnitType<T>`
- **toBasicDataUnitType()**: Returns `BasicDataUnitType<Iterable<T>>`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

## 

### Fields:

- **isPlain()**: Returns `boolean`
- **isSupertypeOf()**: Returns `boolean`

### Public Methods:

- **isPlain()**: Returns `boolean`
- **isSupertypeOf()**: Returns `boolean`

## 

### Fields:

- **Implements**: ReferenceCountable
- **disposeIfUnreferenced()**: Returns `boolean`
- **getNumReferences()**: Returns `int`
- **noteObtainedReference()**: Returns `void`
- **noteDiscardedReference()**: Returns `void`
- **isDisposed()**: Returns `boolean`

### Public Methods:

- **disposeIfUnreferenced()**: Returns `boolean`
- **getNumReferences()**: Returns `int`
- **noteObtainedReference()**: Returns `void`
- **noteDiscardedReference()**: Returns `void`
- **isDisposed()**: Returns `boolean`

## 

## 

### Fields:

**Description**: * Utilities to perform actions.

## 

### Fields:

**Description**: * A mutable bit-mask.
- **Implements**: Cloneable, Iterable
- **cardinalityCache**: `int` (private)
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
- **ensureCapacity()**: Returns `boolean`
- **toHexString()**: Returns `String`
- **toBinString()**: Returns `String`
- **toIndexString()**: Returns `String`

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
- **ensureCapacity()**: Returns `boolean`
- **toHexString()**: Returns `String`
- **toBinString()**: Returns `String`
- **toIndexString()**: Returns `String`

## 

### Fields:

**Description**: * This utility maintains canonical sets of objects.
- **Extends**: T
- **Implements**: Set
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

## 

### Fields:

**Description**: * Utility to expose interfaces that accept a callback as an {@link Iterator}.
- **next**: `T` (private)
- **hasNext()**: Returns `boolean`
- **next()**: Returns `T`
- **getIterator()**: Returns `Iterator<T>`
- **getConsumer()**: Returns `Consumer<T>`
- **declareLastAdd()**: Returns `void`
- **ensureInitialized()**: Returns `void`
- **moveToNext()**: Returns `void`
- **add()**: Returns `void`
- **read()**: Returns `T`

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `T`
- **getIterator()**: Returns `Iterator<T>`
- **getConsumer()**: Returns `Consumer<T>`
- **declareLastAdd()**: Returns `void`
- **ensureInitialized()**: Returns `void`
- **moveToNext()**: Returns `void`
- **add()**: Returns `void`
- **read()**: Returns `T`

## 

## 

### Fields:

**Description**: * This utility helps to count elements.
- **Implements**: Iterable
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
- **addAll()**: Returns `void`

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
- **addAll()**: Returns `void`

## 

### Fields:

**Description**: * Iterates all combinations, i.e., the Cartesian product, of given {@link Iterable}s.
- **Extends**: Iterable
- **Implements**: Iterable
- **vals**: `List<T>` (private)
- **hasEmptyIterator**: `boolean` (private)
- **hasNext()**: Returns `boolean`
- **next()**: Returns `List<T>`

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `List<T>`

## 

### Fields:

**Description**: * Formats different general purpose objects.

## 

### Fields:

**Description**: * Utilities for the work with {@link Iterator}s.

## 

### Fields:

- **Extends**: JsonSerializable
- **Implements**: JsonSerializer
- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `T`

### Public Methods:

- **serialize()**: Returns `WayangJsonObj`
- **deserialize()**: Returns `T`

## 

### Fields:

**Description**: * Utility to deal with {@link JsonSerializable}s.
- **Extends**: T

## 

### Fields:

- **Extends**: T

## 

### Fields:

**Description**: * Utilities to deal with JUEL expressions.
- **apply()**: Returns `T`
- **apply()**: Returns `T`
- **initializeContext()**: Returns `void`

### Public Methods:

- **apply()**: Returns `T`
- **apply()**: Returns `T`
- **initializeContext()**: Returns `void`

## 

### Fields:

**Description**: * {@link InputStream} that is trimmed to a specified size. Moreover, it counts the number of read bytes.
- **Extends**: InputStream
- **read()**: Returns `int`
- **read()**: Returns `int`
- **skip()**: Returns `long`
- **available()**: Returns `int`
- **close()**: Returns `void`
- **markSupported()**: Returns `boolean`
- **getNumReadBytes()**: Returns `long`
- **getMaxBytesToRead()**: Returns `int`

### Public Methods:

- **read()**: Returns `int`
- **read()**: Returns `int`
- **skip()**: Returns `long`
- **available()**: Returns `int`
- **close()**: Returns `void`
- **markSupported()**: Returns `boolean`
- **getNumReadBytes()**: Returns `long`
- **getMaxBytesToRead()**: Returns `int`

## 

## 

### Fields:

**Description**: * Key-value cache with "least recently used" eviction strategy.
- **Extends**: LinkedHashMap
- **getCapacity()**: Returns `int`

### Public Methods:

- **getCapacity()**: Returns `int`

## 

### Fields:

**Description**: * Maps keys to multiple values. Each key value pair is unique.
- **Extends**: HashMap
- **putSingle()**: Returns `boolean`
- **removeSingle()**: Returns `boolean`

### Public Methods:

- **putSingle()**: Returns `boolean`
- **removeSingle()**: Returns `boolean`

## 

## 

### Fields:

- **isAvailable()**: Returns `boolean`
- **getValue()**: Returns `Object`
- **stream()**: Returns `Stream`
- **isAvailable()**: Returns `boolean`
- **getValue()**: Returns `T`
- **stream()**: Returns `Stream<T>`

### Public Methods:

- **isAvailable()**: Returns `boolean`
- **getValue()**: Returns `Object`
- **stream()**: Returns `Stream`
- **isAvailable()**: Returns `boolean`
- **getValue()**: Returns `T`
- **stream()**: Returns `Stream<T>`

## 

## 

### Fields:

**Description**: * Utilities for reflection code.
- **Extends**: T

## 

### Fields:

**Description**: * A helper data structure to manage two values without creating new classes.
- **field0**: `T0` (public)
- **field1**: `T1` (public)
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **setField0()**: Returns `void`
- **getField1()**: Returns `T1`
- **setField1()**: Returns `void`

### Public Methods:

- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`
- **getField0()**: Returns `T0`
- **setField0()**: Returns `void`
- **getField1()**: Returns `T1`
- **setField1()**: Returns `void`

## 

### Fields:

**Description**: * Utility for handling arrays.

## 

### Fields:

**Description**: * Utilities to operate {@link java.util.Collection}s.
- **Extends**: Collection

## 

## 

### Fields:

**Description**: * Tool to work with {@link FileSystem}s.

## 

### Fields:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `String`
- **advance()**: Returns `void`

### Public Methods:

- **hasNext()**: Returns `boolean`
- **next()**: Returns `String`
- **advance()**: Returns `void`

## 

### Fields:

**Description**: * {@link FileSystem} immplementation for the HDFS.
- **Implements**: FileSystem
- **ensureInitialized()**: Returns `void`
- **getFileSize()**: Returns `long`
- **canHandle()**: Returns `boolean`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

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

## 

### Fields:

**Description**: * {@link FileSystem} implementation for the local file system.
- **Implements**: FileSystem
- **getFileSize()**: Returns `long`
- **canHandle()**: Returns `boolean`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`
- **delete()**: Returns `boolean`

### Public Methods:

- **getFileSize()**: Returns `long`
- **canHandle()**: Returns `boolean`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`
- **delete()**: Returns `boolean`

## 

### Fields:

- **Implements**: FileSystem
- **s3**: `AmazonS3` (private)
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
- **getS3Client()**: Returns `AmazonS3`
- **getS3Pair()**: Returns `S3Pair`
- **getFileSize()**: Returns `long`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

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
- **getS3Client()**: Returns `AmazonS3`
- **getS3Pair()**: Returns `S3Pair`
- **getFileSize()**: Returns `long`
- **open()**: Returns `InputStream`
- **create()**: Returns `OutputStream`
- **create()**: Returns `OutputStream`
- **isDirectory()**: Returns `boolean`
- **listChildren()**: Returns `Collection<String>`
- **delete()**: Returns `boolean`

## 

### Fields:

**Description**: * JSONArray is the wrapper for the {@link ArrayNode} to enable the
- **Implements**: Iterable
- **node**: `ArrayNode` (private)
- **length()**: Returns `int`
- **getJSONObject()**: Returns `WayangJsonObj`
- **put()**: Returns `void`
- **put()**: Returns `void`
- **toString()**: Returns `String`
- **iterator()**: Returns `Iterator<Object>`

### Public Methods:

- **length()**: Returns `int`
- **getJSONObject()**: Returns `WayangJsonObj`
- **put()**: Returns `void`
- **put()**: Returns `void`
- **toString()**: Returns `String`
- **iterator()**: Returns `Iterator<Object>`

## 

### Fields:

**Description**: * JSONObject is the wrapper for the {@link ObjectNode} to enable the
- **node**: `ObjectNode` (private)
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

## 

## 

### Fields:

**Description**: * Default {@link Context} implementation that can be configured.
- **Implements**: Context
- **parentContext**: `Context` (private)
- **getVariable()**: Returns `double`
- **setParentContext()**: Returns `void`
- **setVariable()**: Returns `void`
- **setFunction()**: Returns `void`

### Public Methods:

- **getVariable()**: Returns `double`
- **setParentContext()**: Returns `void`
- **setVariable()**: Returns `void`
- **setFunction()**: Returns `void`

## 

## 

### Fields:

**Description**: * This utility builds {@link Expression}s from an input {@link String}.
- **Extends**: MathExBaseVisitor
- **syntaxError()**: Returns `void`
- **syntaxError()**: Returns `void`
- **visitConstant()**: Returns `Expression`
- **visitFunction()**: Returns `Expression`
- **visitVariable()**: Returns `Expression`
- **visitParensExpression()**: Returns `Expression`
- **visitBinaryOperation()**: Returns `Expression`
- **visitUnaryOperation()**: Returns `Expression`

### Public Methods:

- **syntaxError()**: Returns `void`
- **syntaxError()**: Returns `void`
- **visitConstant()**: Returns `Expression`
- **visitFunction()**: Returns `Expression`
- **visitVariable()**: Returns `Expression`
- **visitParensExpression()**: Returns `Expression`
- **visitBinaryOperation()**: Returns `Expression`
- **visitUnaryOperation()**: Returns `Expression`

## 

### Fields:

**Description**: * This exception signals a failed {@link Expression} evaluation.
- **Extends**: MathExException

## 

### Fields:

**Description**: * This exception signals a failed {@link Expression} evaluation.
- **Extends**: RuntimeException

## 

### Fields:

**Description**: * This exception signals a failed {@link Expression} evaluation.
- **Extends**: MathExException

## 

### Fields:

**Description**: * An operation {@link Expression}.
- **Implements**: Expression
- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

### Public Methods:

- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * {@link Expression} implementation that represents a function with a static implementation.
- **Implements**: Expression
- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

### Public Methods:

- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * A constant {@link Expression}.
- **Implements**: Expression
- **getValue()**: Returns `double`
- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

### Public Methods:

- **getValue()**: Returns `double`
- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * {@link Expression} implementation that represents a function that is identified
- **Implements**: Expression
- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

### Public Methods:

- **evaluate()**: Returns `double`
- **specify()**: Returns `Expression`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * An operation {@link Expression}.
- **Implements**: Expression
- **evaluate()**: Returns `double`
- **toString()**: Returns `String`

### Public Methods:

- **evaluate()**: Returns `double`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * A variable {@link Expression}
- **Implements**: Expression
- **evaluate()**: Returns `double`
- **toString()**: Returns `String`

### Public Methods:

- **evaluate()**: Returns `double`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * Test suite for {@link Slot}s.
- **testConnectMismatchingSlotFails()**: Returns `void`
- **testConnectMatchingSlots()**: Returns `void`

### Public Methods:

- **testConnectMismatchingSlotFails()**: Returns `void`
- **testConnectMatchingSlots()**: Returns `void`

## 

### Fields:

**Description**: * Tests for {@link OperatorPattern}.
- **testAdditionalTests()**: Returns `void`

### Public Methods:

- **testAdditionalTests()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link org.apache.wayang.core.mapping.PlanTransformation} class.
- **testReplace()**: Returns `void`
- **testIntroduceAlternative()**: Returns `void`
- **testFlatAlternatives()**: Returns `void`

### Public Methods:

- **testReplace()**: Returns `void`
- **testIntroduceAlternative()**: Returns `void`
- **testFlatAlternatives()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link SubplanPattern}.
- **testMatchSinkPattern()**: Returns `void`
- **testMatchSourcePattern()**: Returns `void`
- **testMatchChainedPattern()**: Returns `void`

### Public Methods:

- **testMatchSinkPattern()**: Returns `void`
- **testMatchSourcePattern()**: Returns `void`
- **testMatchChainedPattern()**: Returns `void`

## 

### Fields:

**Description**: * Dummy {@link Mapping} implementation from {@link TestSink} to {@link TestSink2}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **hashCode()**: Returns `int`
- **equals()**: Returns `boolean`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **hashCode()**: Returns `int`
- **equals()**: Returns `boolean`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * This factory replaces a {@link TestSink} by a
- **Extends**: ReplacementSubplanFactory

## 

### Fields:

**Description**: * Test suite for {@link AggregatingCardinalityEstimator}.
- **testEstimate()**: Returns `void`

### Public Methods:

- **testEstimate()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link DefaultCardinalityEstimator}.
- **testBinaryInputEstimation()**: Returns `void`

### Public Methods:

- **testBinaryInputEstimation()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link LoopSubplanCardinalityPusher}.
- **job**: `Job` (private)
- **configuration**: `Configuration` (private)
- **setUp()**: Returns `void`
- **testWithSingleLoopAndSingleIteration()**: Returns `void`
- **testWithSingleLoopAndManyIteration()**: Returns `void`
- **testWithSingleLoopWithConstantInput()**: Returns `void`
- **testNestedLoops()**: Returns `void`

### Public Methods:

- **setUp()**: Returns `void`
- **testWithSingleLoopAndSingleIteration()**: Returns `void`
- **testWithSingleLoopAndManyIteration()**: Returns `void`
- **testWithSingleLoopWithConstantInput()**: Returns `void`
- **testNestedLoops()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SubplanCardinalityPusher}.
- **job**: `Job` (private)
- **configuration**: `Configuration` (private)
- **setUp()**: Returns `void`
- **testSimpleSubplan()**: Returns `void`
- **testSourceSubplan()**: Returns `void`
- **testDAGShapedSubplan()**: Returns `void`

### Public Methods:

- **setUp()**: Returns `void`
- **testSimpleSubplan()**: Returns `void`
- **testSourceSubplan()**: Returns `void`
- **testDAGShapedSubplan()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link ChannelConversionGraph}.
- **findDirectConversion()**: Returns `void`
- **findIntricateConversion()**: Returns `void`
- **findIntricateConversion2()**: Returns `void`
- **updateExistingConversionWithOnlySourceChannel()**: Returns `void`
- **updateExistingConversionWithReachedDestination()**: Returns `void`
- **updateExistingConversionWithTwoOpenChannels()**: Returns `void`

### Public Methods:

- **findDirectConversion()**: Returns `void`
- **findIntricateConversion()**: Returns `void`
- **findIntricateConversion2()**: Returns `void`
- **updateExistingConversionWithOnlySourceChannel()**: Returns `void`
- **updateExistingConversionWithReachedDestination()**: Returns `void`
- **updateExistingConversionWithTwoOpenChannels()**: Returns `void`

## 

### Fields:

**Description**: * Tests for the {@link NestableLoadProfileEstimator}.
- **Extends**: UnaryToUnaryOperator
- **Implements**: ExecutionOperator
- **testFromJuelSpecification()**: Returns `void`
- **testFromMathExSpecification()**: Returns `void`
- **testFromJuelSpecificationWithImport()**: Returns `void`
- **testMathExFromSpecificationWithImport()**: Returns `void`
- **getNumIterations()**: Returns `int`
- **getPlatform()**: Returns `Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **testFromJuelSpecification()**: Returns `void`
- **testFromMathExSpecification()**: Returns `void`
- **testFromJuelSpecificationWithImport()**: Returns `void`
- **testMathExFromSpecificationWithImport()**: Returns `void`
- **getNumIterations()**: Returns `int`
- **getPlatform()**: Returns `Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Test suite for {@link StageAssignmentTraversal}.
- **testCircularPlatformAssignment()**: Returns `void`
- **testZigZag()**: Returns `void`

### Public Methods:

- **testCircularPlatformAssignment()**: Returns `void`
- **testZigZag()**: Returns `void`

## 

### Fields:

**Description**: * {@link Channel} implementation that can be used for test purposes.
- **Extends**: Channel
- **copy()**: Returns `Channel`
- **createInstance()**: Returns `ChannelInstance`

### Public Methods:

- **copy()**: Returns `Channel`
- **createInstance()**: Returns `ChannelInstance`

## 

### Fields:

**Description**: * Test suite for the {@link LoopIsolator}.
- **testWithSingleLoop()**: Returns `void`
- **testWithSingleLoopWithConstantInput()**: Returns `void`
- **testNestedLoops()**: Returns `void`

### Public Methods:

- **testWithSingleLoop()**: Returns `void`
- **testWithSingleLoopWithConstantInput()**: Returns `void`
- **testNestedLoops()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link Operator} class.
- **Extends**: OperatorBase
- **getOp1Property()**: Returns `double`
- **getOp2Property()**: Returns `double`
- **testPropertyDetection()**: Returns `void`
- **testPropertyCollection()**: Returns `void`

### Public Methods:

- **getOp1Property()**: Returns `double`
- **getOp2Property()**: Returns `double`
- **testPropertyDetection()**: Returns `void`
- **testPropertyCollection()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SlotMapping}.
- **testSimpleSlotMapping()**: Returns `void`
- **testOverridingSlotMapping()**: Returns `void`
- **testMultiMappings()**: Returns `void`

### Public Methods:

- **testSimpleSlotMapping()**: Returns `void`
- **testOverridingSlotMapping()**: Returns `void`
- **testMultiMappings()**: Returns `void`

## 

### Fields:

**Description**: * Test operator that exposes map-like behavior. Does not provide a {@link CardinalityEstimator}.
- **Extends**: UnaryToUnaryOperator

## 

### Fields:

**Description**: * Test operator that exposes filter-like behavior.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **isSupportingBroadcastInputs()**: Returns `boolean`
- **getSelectivity()**: Returns `double`
- **setSelectivity()**: Returns `void`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **isSupportingBroadcastInputs()**: Returns `boolean`
- **getSelectivity()**: Returns `double`
- **setSelectivity()**: Returns `void`

## 

### Fields:

**Description**: * Join-like operator.
- **Extends**: BinaryToUnaryOperator
- **Implements**: ElementaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * {@link LoopHeadOperator} implementation for test purposes.
- **Extends**: OperatorBase
- **Implements**: LoopHeadOperator, ElementaryOperator
- **numExpectedIterations**: `int` (private)
- **getNumExpectedIterations()**: Returns `int`
- **setNumExpectedIterations()**: Returns `void`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

### Public Methods:

- **getNumExpectedIterations()**: Returns `int`
- **setNumExpectedIterations()**: Returns `void`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`

## 

### Fields:

**Description**: * Test operator that exposes map-like behavior.
- **Extends**: UnaryToUnaryOperator
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **isSupportingBroadcastInputs()**: Returns `boolean`

### Public Methods:

- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **isSupportingBroadcastInputs()**: Returns `boolean`

## 

### Fields:

**Description**: * Another dummy sink for testing purposes.
- **Extends**: UnarySink

## 

### Fields:

**Description**: * Test suites for {@link PartialExecution}s.
- **testJsonSerialization()**: Returns `void`

### Public Methods:

- **testJsonSerialization()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link DynamicPlugin} class.
- **testLoadYaml()**: Returns `void`
- **testPartialYaml()**: Returns `void`
- **testEmptyYaml()**: Returns `void`
- **testExclusion()**: Returns `void`

### Public Methods:

- **testLoadYaml()**: Returns `void`
- **testPartialYaml()**: Returns `void`
- **testEmptyYaml()**: Returns `void`
- **testExclusion()**: Returns `void`

## 

### Fields:

**Description**: * Dummy {@link ExecutionOperator} for test purposes.
- **Extends**: OperatorBase
- **Implements**: ExecutionOperator
- **someProperty**: `int` (private)
- **getSomeProperty()**: Returns `int`
- **setSomeProperty()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getSomeProperty()**: Returns `int`
- **setSomeProperty()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Dummy {@link Channel}.
- **Extends**: Channel
- **copy()**: Returns `DummyExternalReusableChannel`
- **createInstance()**: Returns `ChannelInstance`

### Public Methods:

- **copy()**: Returns `DummyExternalReusableChannel`
- **createInstance()**: Returns `ChannelInstance`

## 

### Fields:

**Description**: * Dummy {@link Channel}.
- **Extends**: Channel
- **copy()**: Returns `DummyNonReusableChannel`
- **createInstance()**: Returns `ChannelInstance`

### Public Methods:

- **copy()**: Returns `DummyNonReusableChannel`
- **createInstance()**: Returns `ChannelInstance`

## 

### Fields:

**Description**: * {@link Platform} implementation for test purposes.
- **Extends**: Platform
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **convert()**: Returns `TimeEstimate`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **convert()**: Returns `TimeEstimate`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

## 

### Fields:

**Description**: * Dummy {@link Channel}.
- **Extends**: Channel
- **copy()**: Returns `Channel`
- **createInstance()**: Returns `ChannelInstance`

### Public Methods:

- **copy()**: Returns `Channel`
- **createInstance()**: Returns `ChannelInstance`

## 

### Fields:

**Description**: * Utility to mock Wayang objects.

## 

### Fields:

**Description**: * Dummy {@link ExecutionOperator} for test purposes.
- **Extends**: DummyExecutionOperator
- **Implements**: JsonSerializable
- **toJson()**: Returns `WayangJsonObj`

### Public Methods:

- **toJson()**: Returns `WayangJsonObj`

## 

### Fields:

**Description**: * A data unit type for test purposes.

## 

### Fields:

**Description**: * Another data unit type for test purposes.

## 

### Fields:

**Description**: * Test suite for {@link Bitmask}s.
- **testEquals()**: Returns `void`
- **testFlip()**: Returns `void`
- **testIsSubmaskOf()**: Returns `void`
- **testCardinality()**: Returns `void`
- **testOr()**: Returns `void`
- **testAndNot()**: Returns `void`
- **testNextSetBit()**: Returns `void`
- **testFlip()**: Returns `void`
- **testSetBits()**: Returns `void`

### Public Methods:

- **testEquals()**: Returns `void`
- **testFlip()**: Returns `void`
- **testIsSubmaskOf()**: Returns `void`
- **testCardinality()**: Returns `void`
- **testOr()**: Returns `void`
- **testAndNot()**: Returns `void`
- **testNextSetBit()**: Returns `void`
- **testFlip()**: Returns `void`
- **testSetBits()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link ConsumerIteratorAdapter}.
- **testCriticalLoad()**: Returns `void`

### Public Methods:

- **testCriticalLoad()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link CrossProductIterable}.
- **test2x3()**: Returns `void`
- **test3x2()**: Returns `void`
- **test1x3()**: Returns `void`
- **test3x1()**: Returns `void`
- **test1x1()**: Returns `void`
- **test0x0()**: Returns `void`
- **test2and0()**: Returns `void`

### Public Methods:

- **test2x3()**: Returns `void`
- **test3x2()**: Returns `void`
- **test1x3()**: Returns `void`
- **test3x1()**: Returns `void`
- **test1x1()**: Returns `void`
- **test0x0()**: Returns `void`
- **test2and0()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link LimitedInputStream}.
- **testLimitation()**: Returns `void`

### Public Methods:

- **testLimitation()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link ReflectionUtils}.
- **Extends**: MyParameterizedClassA
- **Implements**: MyParameterizedInterface
- **testEvaluateWithInstantiation()**: Returns `void`
- **testEvaluateWithStaticVariable()**: Returns `void`
- **testEvaluateWithStaticMethod()**: Returns `void`
- **testGetTypeParametersWithReusedTypeParameters()**: Returns `void`
- **testGetTypeParametersWithIndirectTypeParameters()**: Returns `void`

### Public Methods:

- **testEvaluateWithInstantiation()**: Returns `void`
- **testEvaluateWithStaticVariable()**: Returns `void`
- **testEvaluateWithStaticMethod()**: Returns `void`
- **testGetTypeParametersWithReusedTypeParameters()**: Returns `void`
- **testGetTypeParametersWithIndirectTypeParameters()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link WayangCollections}.
- **testCreatePowerList()**: Returns `void`

### Public Methods:

- **testCreatePowerList()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the class {@link ExpressionBuilder}.
- **shouldNotFailOnValidInput()**: Returns `void`
- **shouldFailOnInvalidInput()**: Returns `void`

### Public Methods:

- **shouldNotFailOnValidInput()**: Returns `void`
- **shouldFailOnInvalidInput()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link Expression} subclasses.
- **testSingletonExpressions()**: Returns `void`
- **testFailsOnMissingContext()**: Returns `void`
- **testComplexExpressions()**: Returns `void`
- **testSpecification()**: Returns `void`

### Public Methods:

- **testSingletonExpressions()**: Returns `void`
- **testFailsOnMissingContext()**: Returns `void`
- **testComplexExpressions()**: Returns `void`
- **testSpecification()**: Returns `void`

## 

### Fields:

**Description**: * This class provides facilities to save and load {@link Experiment}s.
- **Extends**: Measurement
- **storage**: `Storage` (private)
- **gson**: `Gson` (private)
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

## 

### Fields:

**Description**: * Utility to create {@link TimeMeasurement}s for an {@link Experiment}.
- **getOrCreateRound()**: Returns `TimeMeasurement`
- **start()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`
- **toPrettyString()**: Returns `String`
- **toPrettyString()**: Returns `String`
- **stopAll()**: Returns `void`
- **getExperiment()**: Returns `Experiment`
- **determineFirstColumnWidth()**: Returns `int`
- **determineFirstColumnWidth()**: Returns `int`
- **append()**: Returns `void`

### Public Methods:

- **getOrCreateRound()**: Returns `TimeMeasurement`
- **start()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`
- **toPrettyString()**: Returns `String`
- **toPrettyString()**: Returns `String`
- **stopAll()**: Returns `void`
- **getExperiment()**: Returns `Experiment`
- **determineFirstColumnWidth()**: Returns `int`
- **determineFirstColumnWidth()**: Returns `int`
- **append()**: Returns `void`

## 

### Fields:

**Description**: * Custom deserializer for {@link Measurement}s
- **Extends**: Measurement
- **Implements**: JsonDeserializer
- **register()**: Returns `void`
- **deserialize()**: Returns `Measurement`

### Public Methods:

- **register()**: Returns `void`
- **deserialize()**: Returns `Measurement`

## 

### Fields:

**Description**: * Custom serializer for {@link Measurement}s
- **Implements**: JsonSerializer
- **serialize()**: Returns `JsonElement`

### Public Methods:

- **serialize()**: Returns `JsonElement`

## 

### Fields:

**Description**: * An experiment comprises {@link Measurement}s from one specific {@link Subject} execution.
- **id**: `String` (private)
- **description**: `String` (private)
- **startTime**: `long` (private)
- **tags**: `Collection<String>` (private)
- **measurements**: `Collection<Measurement>` (private)
- **subject**: `Subject` (private)
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

## 

### Fields:

- **Extends**: Measurement
- **id**: `String` (private)
- **getId()**: Returns `String`
- **setId()**: Returns `void`
- **getType()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Public Methods:

- **getId()**: Returns `String`
- **setId()**: Returns `void`
- **getType()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

## 

### Fields:

**Description**: * The subject of an {@link Experiment}, e.g., an application or algorithm.
- **id**: `String` (private)
- **version**: `String` (private)
- **addConfiguration()**: Returns `Subject`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Public Methods:

- **addConfiguration()**: Returns `Subject`
- **toString()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

## 

## 

### Fields:

- **Extends**: Measurement
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
- **ensureStarted()**: Returns `void`
- **start()**: Returns `TimeMeasurement`
- **getRound()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`

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
- **ensureStarted()**: Returns `void`
- **start()**: Returns `TimeMeasurement`
- **getRound()**: Returns `TimeMeasurement`
- **stop()**: Returns `void`

## 

### Fields:

- **Extends**: Storage
- **file**: `File` (private)
- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **append()**: Returns `void`
- **append()**: Returns `void`

### Public Methods:

- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **append()**: Returns `void`
- **append()**: Returns `void`

## 

### Fields:

- **Extends**: Storage
- **file**: `File` (private)
- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **append()**: Returns `void`
- **append()**: Returns `void`

### Public Methods:

- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **append()**: Returns `void`
- **append()**: Returns `void`

## 

### Fields:

- **storageFile**: `URI` (private)
- **context**: `ProfileDB` (private)
- **setContext()**: Returns `void`
- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **load()**: Returns `Collection<Experiment>`

### Public Methods:

- **setContext()**: Returns `void`
- **changeLocation()**: Returns `void`
- **save()**: Returns `void`
- **save()**: Returns `void`
- **load()**: Returns `Collection<Experiment>`
- **load()**: Returns `Collection<Experiment>`

## 

### Fields:

- **testPolymorphSaveAndLoad()**: Returns `void`
- **testRecursiveSaveAndLoad()**: Returns `void`
- **testFileOperations()**: Returns `void`
- **testAppendOnNonExistentFile()**: Returns `void`

### Public Methods:

- **testPolymorphSaveAndLoad()**: Returns `void`
- **testRecursiveSaveAndLoad()**: Returns `void`
- **testFileOperations()**: Returns `void`
- **testAppendOnNonExistentFile()**: Returns `void`

## 

### Fields:

- **Extends**: Measurement
- **timestamp**: `long` (private)
- **usedMb**: `long` (private)
- **getTimestamp()**: Returns `long`
- **setTimestamp()**: Returns `void`
- **getUsedMb()**: Returns `long`
- **setUsedMb()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

### Public Methods:

- **getTimestamp()**: Returns `long`
- **setTimestamp()**: Returns `void`
- **getUsedMb()**: Returns `long`
- **setUsedMb()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`
- **toString()**: Returns `String`

## 

### Fields:

- **Extends**: Measurement
- **millis**: `long` (private)
- **submeasurements**: `Collection<Measurement>` (private)
- **getMillis()**: Returns `long`
- **setMillis()**: Returns `void`
- **getSubmeasurements()**: Returns `Collection<Measurement>`
- **addSubmeasurements()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Public Methods:

- **getMillis()**: Returns `long`
- **setMillis()**: Returns `void`
- **getSubmeasurements()**: Returns `Collection<Measurement>`
- **addSubmeasurements()**: Returns `void`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

---

# wayang-docs

---

# wayang-platforms

## # wayang-platforms

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

**Description**: * Register for the {@link ChannelConversion}s supported for this platform.

## 

### Fields:

**Description**: * Describes the situation where one {@link DataSet} is operated on, producing a further {@link DataSet}.
- **Extends**: Channel
- **size**: `long` (private)
- **copy()**: Returns `Channel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **getChannel()**: Returns `DataSetChannel`

### Public Methods:

- **copy()**: Returns `Channel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **getChannel()**: Returns `DataSetChannel`

## 

### Fields:

**Description**: * Wrapper of {@Link CoGroupFunction} of Flink for use in Wayang
- **Implements**: CoGroupFunction
- **coGroup()**: Returns `void`

### Public Methods:

- **coGroup()**: Returns `void`

## 

### Fields:

**Description**: * A compiler translates Wayang functions into executable Java functions.
- **Implements**: PairFunction
- **getWayangFunction()**: Returns `Object`
- **call()**: Returns `Type`
- **getWayangFunction()**: Returns `Object`

### Public Methods:

- **getWayangFunction()**: Returns `Object`
- **call()**: Returns `Type`
- **getWayangFunction()**: Returns `Object`

## 

### Fields:

**Description**: * Wrapper for {@Link KeySelector}
- **Implements**: KeySelector
- **getKey()**: Returns `String`

### Public Methods:

- **getKey()**: Returns `String`

## 

### Fields:

**Description**: * Wrapper for {@Link KeySelector}
- **Implements**: KeySelector
- **key**: `Class<K>` (public)
- **typeInformation**: `TypeInformation<K>` (public)
- **getKey()**: Returns `K`
- **getProducedType()**: Returns `TypeInformation`

### Public Methods:

- **getKey()**: Returns `K`
- **getProducedType()**: Returns `TypeInformation`

## 

### Fields:

**Description**: * Wrapper for {@Link OutputFormat}
- **Implements**: OutputFormat
- **configure()**: Returns `void`
- **open()**: Returns `void`
- **writeRecord()**: Returns `void`
- **close()**: Returns `void`

### Public Methods:

- **configure()**: Returns `void`
- **open()**: Returns `void`
- **writeRecord()**: Returns `void`
- **close()**: Returns `void`

## 

### Fields:

**Description**: * Wrapper for {@link FileOutputFormat}
- **Extends**: FileOutputFormat
- **Implements**: InitializeOnMaster, CleanupWhenUnsuccessful
- **blockPos**: `int` (private)
- **headerStream**: `DataOutputView` (private)
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
- **writeInfo()**: Returns `void`

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
- **writeInfo()**: Returns `void`

## 

### Fields:

**Description**: * Create a {@Link FilterFunction} that remove the elements null
- **Implements**: FilterFunction
- **filter()**: Returns `boolean`
- **getProducedType()**: Returns `TypeInformation<InputType>`

### Public Methods:

- **filter()**: Returns `boolean`
- **getProducedType()**: Returns `TypeInformation<InputType>`

## 

### Fields:

**Description**: * Class create a {@Link MapFunction} that genereta only null as convertion
- **Implements**: MapFunction
- **map()**: Returns `OutputType`
- **getProducedType()**: Returns `TypeInformation<OutputType>`

### Public Methods:

- **map()**: Returns `OutputType`
- **getProducedType()**: Returns `TypeInformation<OutputType>`

## 

### Fields:

**Description**: * Class create a {@Link Aggregator} that generate aggregatorWrapper
- **Implements**: Aggregator
- **elements**: `List<WayangValue>` (private)
- **getAggregate()**: Returns `ListValue<WayangValue>`
- **aggregate()**: Returns `void`
- **aggregate()**: Returns `void`
- **reset()**: Returns `void`

### Public Methods:

- **getAggregate()**: Returns `ListValue<WayangValue>`
- **aggregate()**: Returns `void`
- **aggregate()**: Returns `void`
- **reset()**: Returns `void`

## 

### Fields:

**Description**: * Class create a {@Link ConvergenceCriterion} that generate aggregatorWrapper
- **Implements**: ConvergenceCriterion
- **doWhile**: `boolean` (private)
- **setDoWhile()**: Returns `WayangConvergenceCriterion`
- **isConverged()**: Returns `boolean`

### Public Methods:

- **setDoWhile()**: Returns `WayangConvergenceCriterion`
- **isConverged()**: Returns `boolean`

## 

### Fields:

**Description**: * Class create a {@Link FilterFunction} for use inside of the LoopOperators
- **Extends**: AbstractRichFunction
- **Implements**: FilterFunction
- **wayangAggregator**: `WayangAggregator` (private)
- **name**: `String` (private)
- **open()**: Returns `void`
- **filter()**: Returns `boolean`

### Public Methods:

- **open()**: Returns `void`
- **filter()**: Returns `boolean`

## 

### Fields:

**Description**: * Is a Wrapper for used in the criterion of the Loops
- **Extends**: ListValue

## 

### Fields:

**Description**: * Implementation of {@link Value} of flink for use in Wayang
- **Implements**: Value
- **data**: `T` (private)
- **write()**: Returns `void`
- **read()**: Returns `void`
- **convertToObject()**: Returns `T`
- **toString()**: Returns `String`
- **get()**: Returns `T`

### Public Methods:

- **write()**: Returns `void`
- **read()**: Returns `void`
- **convertToObject()**: Returns `T`
- **toString()**: Returns `String`
- **get()**: Returns `T`

## 

### Fields:

**Description**: * Wraps and manages a Flink {@link ExecutionEnvironment} to avoid steady re-creation.
- **Extends**: ExecutionResourceTemplate
- **flinkEnviroment**: `ExecutionEnvironment` (private)
- **get()**: Returns `ExecutionEnvironment`
- **isDisposed()**: Returns `boolean`
- **loadConfiguration()**: Returns `void`
- **getExecutionMode()**: Returns `ExecutionMode`

### Public Methods:

- **get()**: Returns `ExecutionEnvironment`
- **isDisposed()**: Returns `boolean`
- **loadConfiguration()**: Returns `void`
- **getExecutionMode()**: Returns `ExecutionMode`

## 

### Fields:

**Description**: * {@link ExecutionContext} implementation for the {@link FlinkPlatform}.
- **Implements**: ExecutionContext, Serializable
- **richFunction**: `RichFunction` (private)
- **setRichFunction()**: Returns `void`
- **getCurrentIteration()**: Returns `int`

### Public Methods:

- **setRichFunction()**: Returns `void`
- **getCurrentIteration()**: Returns `int`

## 

### Fields:

**Description**: * {@link Executor} implementation for the {@link FlinkPlatform}.
- **Extends**: PushExecutorTemplate
- **flinkContextReference**: `FlinkContextReference` (private)
- **fee**: `ExecutionEnvironment` (public)
- **platform**: `FlinkPlatform` (private)
- **numDefaultPartitions**: `int` (private)
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **getCompiler()**: Returns `FunctionCompiler`
- **getNumDefaultPartitions()**: Returns `int`

### Public Methods:

- **dispose()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **getCompiler()**: Returns `FunctionCompiler`
- **getNumDefaultPartitions()**: Returns `int`

## 

### Fields:

**Description**: * Mapping from {@link CartesianOperator} to {@link SparkCartesianOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link CoGroupOperator} to {@link SparkCoGroupOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link CountOperator} to {@link SparkCountOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link DistinctOperator} to {@link SparkDistinctOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link IntersectOperator} to {@link SparkIntersectOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link JoinOperator} to {@link SparkJoinOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link LocalCallbackSink} to {@link SparkLocalCallbackSink}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Register for the {@link Mapping}s supported for this platform.

## 

### Fields:

**Description**: * Mapping from {@link MaterializedGroupByOperator} to {@link SparkMaterializedGroupByOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link ObjectFileSink} to {@link SparkObjectFileSink}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link ObjectFileSource} to {@link SparkObjectFileSource}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link PageRankOperator} to org.apache.wayang.spark.operators.graph.SparkPageRankOperator .
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link SortOperator} to {@link SparkSortOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link UnionAllOperator} to {@link SparkUnionAllOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Mapping from {@link ZipWithIdOperator} to {@link SparkZipWithIdOperator}.
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

**Description**: * Flink implementation of the {@link CartesianOperator}.
- **Extends**: CartesianOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link CoGroupOperator}.
- **Extends**: CoGroupOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationTypeKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationTypeKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Converts {@link DataSetChannel} into a {@link CollectionChannel}
- **Extends**: UnaryToUnaryOperator
- **Implements**: FlinkExecutionOperator
- **containsAction()**: Returns `boolean`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

## 

### Fields:

**Description**: * This is execution operator implements the {@link CollectionSource}.
- **Extends**: CollectionSource
- **Implements**: the
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Flink implementation of the {@link CountOperator}.
- **Extends**: CountOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link DistinctOperator}.
- **Extends**: DistinctOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link DoWhileOperator}.
- **Extends**: DoWhileOperator
- **Implements**: FlinkExecutionOperator
- **iterativeDataSet**: `IterativeDataSet` (private)
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

- **Extends**: ExecutionOperator

## 

### Fields:

**Description**: * Flink implementation of the {@link FilterOperator}.
- **Extends**: FilterOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link FlatMapOperator}.
- **Extends**: FlatMapOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link GlobalMaterializedGroupOperator}.
- **Extends**: GlobalMaterializedGroupOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link GlobalReduceOperator}.
- **Extends**: GlobalReduceOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link GroupByOperator}.
- **Extends**: GroupByOperator
- **Implements**: FlinkExecutionOperator
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Flink implementation of the {@link IntersectOperator}.
- **Extends**: IntersectOperator
- **Implements**: FlinkExecutionOperator
- **map()**: Returns `Type`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **map()**: Returns `Type`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link JoinOperator}.
- **Extends**: JoinOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Implementation of the {@link LocalCallbackSink} operator for the Flink platform.
- **Extends**: Serializable
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link RepeatOperator}.
- **Extends**: LoopOperator
- **Implements**: FlinkExecutionOperator
- **iterativeDataSet**: `IterativeDataSet` (private)
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Flink implementation of the {@link MapOperator}.
- **Extends**: MapOperator
- **Implements**: FlinkExecutionOperator
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Flink implementation of the {@link MapPartitionsOperator}.
- **Extends**: MapPartitionsOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link MaterializedGroupByOperator}.
- **Extends**: MaterializedGroupByOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
- **Extends**: ObjectFileSink
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
- **Extends**: ObjectFileSource
- **Implements**: FlinkExecutionOperator
- **flatMap()**: Returns `void`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **flatMap()**: Returns `void`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link PageRankOperator}.
- **Extends**: PageRankOperator
- **Implements**: FlinkExecutionOperator
- **flatMap()**: Returns `void`
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **reduce()**: Returns `void`
- **flatMap()**: Returns `void`
- **filter()**: Returns `boolean`

### Public Methods:

- **flatMap()**: Returns `void`
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **reduce()**: Returns `void`
- **flatMap()**: Returns `void`
- **filter()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link ReduceByOperator}.
- **Extends**: ReduceByOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link RepeatOperator}.
- **Extends**: RepeatOperator
- **Implements**: FlinkExecutionOperator
- **iterativeDataSet**: `IterativeDataSet<Type>` (private)
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Flink implementation of the {@link RepeatOperator}.
- **Extends**: RepeatOperator
- **Implements**: FlinkExecutionOperator
- **iterationCounter**: `int` (private)
- **iterativeDataSet**: `IterativeDataSet<Type>` (private)
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Flink implementation of the {@link SampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)
- **Extends**: SampleOperator
- **Implements**: FlinkExecutionOperator
- **rand**: `Random` (private)
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link SortOperator}.
- **Extends**: SortOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Implementation of the {@link TextFileSink} operator for the Flink platform.
- **Extends**: TextFileSink
- **Implements**: FlinkExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

## 

### Fields:

**Description**: * Provides a {@link Collection} to a Flink job.
- **Extends**: TextFileSource
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Created by bertty on 31-10-17.
- **Extends**: Tuple2
- **Implements**: FlinkExecutionOperator
- **dataQuantum**: `Type` (private)
- **map()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **map()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link UnionAllOperator}.
- **Extends**: UnionAllOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Flink implementation of the {@link MapOperator}.
- **Extends**: ZipWithIdOperator
- **Implements**: FlinkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Platform} for Apache Flink.
- **Extends**: Platform
- **getFlinkContext()**: Returns `FlinkContextReference`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

### Public Methods:

- **getFlinkContext()**: Returns `FlinkContextReference`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link FlinkPlatform}.
- **Implements**: Plugin
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link FlinkPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link FlinkPlatform}.

## 

### Fields:

**Description**: * Test suite for {@link FlinkCartesianOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkCoGroupOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`
- **compare()**: Returns `boolean`

### Public Methods:

- **testExecution()**: Returns `void`
- **compare()**: Returns `boolean`

## 

### Fields:

**Description**: * Test suite for the {@link FlinkCollectionSource}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkCountOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkFilterOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkFlatMapOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkGlobalMaterializedGroupOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkGlobalReduceOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkJoinOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkMapPartitionsOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkMaterializedGroupByOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test base for {@link FlinkExecutionOperator} tests.
- **setUp()**: Returns `void`
- **getEnv()**: Returns `ExecutionEnvironment`

### Public Methods:

- **setUp()**: Returns `void`
- **getEnv()**: Returns `ExecutionEnvironment`

## 

### Fields:

**Description**: * Test suite for {@link FlinkReduceByOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkSortOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link FlinkUnionAllOperator}.
- **Extends**: FlinkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Utility to create {@link Channel}s in tests.
- **setUp()**: Returns `void`

### Public Methods:

- **setUp()**: Returns `void`

## 

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

**Description**: * Basic PageRank implementation.
- **Extends**: BasicComputation
- **compute()**: Returns `void`
- **preApplication()**: Returns `void`
- **postApplication()**: Returns `void`
- **preSuperstep()**: Returns `void`
- **postSuperstep()**: Returns `void`
- **initialize()**: Returns `void`
- **nextVertex()**: Returns `boolean`
- **createVertexWriter()**: Returns `TextVertexWriter`
- **writeVertex()**: Returns `void`

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

## 

### Fields:

**Description**: * Parameters for Basic PageRank implementation.

## 

### Fields:

**Description**: * {@link Executor} for the {@link GiraphPlatform}.
- **Extends**: ExecutorTemplate
- **configuration**: `Configuration` (private)
- **job**: `Job` (private)
- **giraphConfiguration**: `GiraphConfiguration` (private)
- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `GiraphPlatform`
- **getGiraphConfiguration()**: Returns `GiraphConfiguration`
- **execute()**: Returns `void`

### Public Methods:

- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `GiraphPlatform`
- **getGiraphConfiguration()**: Returns `GiraphConfiguration`
- **execute()**: Returns `void`

## 

### Fields:

- **Extends**: ExecutionOperator

## 

### Fields:

**Description**: * PageRank {@link Operator} implementation for the {@link GiraphPlatform}.
- **Extends**: PageRankOperator
- **Implements**: GiraphExecutionOperator
- **path_out**: `String` (private)
- **getPlatform()**: Returns `Platform`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **setPathOut()**: Returns `void`
- **getPathOut()**: Returns `String`

### Public Methods:

- **getPlatform()**: Returns `Platform`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **setPathOut()**: Returns `void`
- **getPathOut()**: Returns `String`

## 

### Fields:

**Description**: * Giraph {@link Platform} for Wayang.
- **Extends**: Platform
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **initialize()**: Returns `void`

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **initialize()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} activates default capabilities of the {@link GiraphPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * Test For GiraphPageRank
- **setUp()**: Returns `void`
- **testExecution()**: Returns `void`

### Public Methods:

- **setUp()**: Returns `void`
- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

**Description**: * {@link Executor} for the {@link GraphChiPlatform}.
- **Extends**: ExecutorTemplate
- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `GraphChiPlatform`
- **execute()**: Returns `void`

### Public Methods:

- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `GraphChiPlatform`
- **execute()**: Returns `void`

## 

### Fields:

- **Extends**: ExecutionOperator

## 

### Fields:

**Description**: * PageRank {@link Operator} implementation for the {@link GraphChiPlatform}.
- **Extends**: PageRankOperator
- **Implements**: GraphChiExecutionOperator
- **getPlatform()**: Returns `Platform`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getPlatform()**: Returns `Platform`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * GraphChi {@link Platform} for Wayang.
- **Extends**: Platform
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **initialize()**: Returns `void`

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **initialize()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} activates default capabilities of the {@link GraphChiPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link GraphChiPageRankOperator}.
- **setUp()**: Returns `void`
- **testExecution()**: Returns `void`

### Public Methods:

- **setUp()**: Returns `void`
- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

**Description**: * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.
- **Extends**: Channel
- **Implements**: JavaChannelInstance
- **copy()**: Returns `CollectionChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getChannel()**: Returns `Channel`

### Public Methods:

- **copy()**: Returns `CollectionChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getChannel()**: Returns `Channel`

## 

### Fields:

- **Extends**: ChannelInstance

## 

### Fields:

**Description**: * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.
- **Extends**: Channel
- **Implements**: JavaChannelInitializer
- **copy()**: Returns `StreamChannel`
- **exchangeWith()**: Returns `Channel`
- **createInstance()**: Returns `Instance`
- **setUpOutput()**: Returns `Channel`
- **provideStreamChannel()**: Returns `StreamChannel`
- **accept()**: Returns `void`
- **getChannel()**: Returns `Channel`
- **getMeasuredCardinality()**: Returns `OptionalLong`

### Public Methods:

- **copy()**: Returns `StreamChannel`
- **exchangeWith()**: Returns `Channel`
- **createInstance()**: Returns `Instance`
- **setUpOutput()**: Returns `Channel`
- **provideStreamChannel()**: Returns `StreamChannel`
- **accept()**: Returns `void`
- **getChannel()**: Returns `Channel`
- **getMeasuredCardinality()**: Returns `OptionalLong`

## 

### Fields:

**Description**: * {@link ExecutionContext} implementation for the {@link JavaPlatform}.
- **Implements**: ExecutionContext
- **getCurrentIteration()**: Returns `int`

### Public Methods:

- **getCurrentIteration()**: Returns `int`

## 

### Fields:

**Description**: * {@link Executor} implementation for the {@link JavaPlatform}.
- **Extends**: PushExecutorTemplate
- **getPlatform()**: Returns `JavaPlatform`
- **getCompiler()**: Returns `FunctionCompiler`

### Public Methods:

- **getPlatform()**: Returns `JavaPlatform`
- **getCompiler()**: Returns `FunctionCompiler`

## 

### Fields:

**Description**: * Java implementation of the {@link CartesianOperator}.
- **Extends**: CartesianOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link CoGroupOperator}.
- **Extends**: CoGroupOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Converts {@link StreamChannel} into a {@link CollectionChannel}
- **Extends**: UnaryToUnaryOperator
- **Implements**: JavaExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

## 

### Fields:

**Description**: * This is execution operator implements the {@link TextFileSource}.
- **Extends**: CollectionSource
- **Implements**: the
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link CountOperator}.
- **Extends**: CountOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link DistinctOperator}.
- **Extends**: DistinctOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link DoWhileOperator}.
- **Extends**: DoWhileOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

- **Extends**: ExecutionOperator

## 

### Fields:

**Description**: * Java implementation of the {@link FilterOperator}.
- **Extends**: FilterOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link FlatMapOperator}.
- **Extends**: FlatMapOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link GlobalMaterializedGroupOperator}.
- **Extends**: GlobalMaterializedGroupOperator
- **Implements**: JavaExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

## 

### Fields:

**Description**: * Java implementation of the {@link GlobalReduceOperator}.
- **Extends**: GlobalReduceOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link IntersectOperator}.
- **Extends**: IntersectOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createProbingTable()**: Returns `Set<Type>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createProbingTable()**: Returns `Set<Type>`

## 

### Fields:

**Description**: * Java implementation of the {@link JoinOperator}.
- **Extends**: JoinOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Implementation of the {@link LocalCallbackSink} operator for the Java platform.
- **Extends**: Serializable
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link LoopOperator}.
- **Extends**: LoopOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link org.apache.wayang.basic.operators.MapOperator}.
- **Extends**: MapOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link MapPartitionsOperator}.
- **Extends**: MapPartitionsOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link MaterializedGroupByOperator}.
- **Extends**: MaterializedGroupByOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
- **Extends**: ObjectFileSink
- **Implements**: JavaExecutionOperator
- **nextIndex**: `int` (private)
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **push()**: Returns `void`
- **fire()**: Returns `void`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **push()**: Returns `void`
- **fire()**: Returns `void`

## 

### Fields:

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
- **Extends**: ObjectFileSource
- **Implements**: JavaExecutionOperator
- **nextElements_cole**: `ArrayList` (private)
- **nextIndex**: `int` (private)
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `T`
- **close()**: Returns `void`
- **tryAdvance()**: Returns `void`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `T`
- **close()**: Returns `void`
- **tryAdvance()**: Returns `void`

## 

### Fields:

**Description**: * Java implementation of the {@link JavaRandomSampleOperator}. This sampling method is with replacement (i.e., duplicates may appear in the sample).
- **Extends**: SampleOperator
- **Implements**: JavaExecutionOperator
- **rand**: `Random` (private)
- **test()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **test()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link ReduceByOperator}.
- **Extends**: ReduceByOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **supplier()**: Returns `Supplier<List<T>>`
- **combiner()**: Returns `BinaryOperator<List<T>>`
- **characteristics()**: Returns `Set<Characteristics>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **supplier()**: Returns `Supplier<List<T>>`
- **combiner()**: Returns `BinaryOperator<List<T>>`
- **characteristics()**: Returns `Set<Characteristics>`

## 

### Fields:

**Description**: * Java implementation of the {@link DoWhileOperator}.
- **Extends**: RepeatOperator
- **Implements**: JavaExecutionOperator
- **iterationCounter**: `int` (private)
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link JavaReservoirSampleOperator}.
- **Extends**: SampleOperator
- **Implements**: JavaExecutionOperator
- **rand**: `Random` (private)
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link SortOperator}.
- **Extends**: SortOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Implementation fo the {@link TextFileSink} for the {@link JavaPlatform}.
- **Extends**: TextFileSink
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * This is execution operator implements the {@link TextFileSource}.
- **Extends**: TextFileSource
- **Implements**: the
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **copy()**: Returns `JavaTextFileSource`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **copy()**: Returns `JavaTextFileSource`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a TSV file.
- **Extends**: Tuple2
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
- **Extends**: UnarySource
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createStream()**: Returns `Stream<T>`
- **lineParse()**: Returns `T`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **createStream()**: Returns `Stream<T>`
- **lineParse()**: Returns `T`

## 

### Fields:

**Description**: * Java implementation of the {@link UnionAllOperator}.
- **Extends**: UnionAllOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link PageRankOperator}.
- **Extends**: PageRankOperator
- **Implements**: JavaExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link Platform} for a single JVM executor based on the {@link java.util.stream.Stream} library.
- **Extends**: Platform
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

### Public Methods:

- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} is a subset of the {@link JavaBasicPlugin} and only ships with the {@link ChannelConversion}s.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for the {@link JavaExecutor}.
- **increment**: `int` (private)
- **testLazyExecutionResourceHandling()**: Returns `void`
- **apply()**: Returns `Integer`
- **open()**: Returns `void`

### Public Methods:

- **testLazyExecutionResourceHandling()**: Returns `void`
- **apply()**: Returns `Integer`
- **open()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaCartesianOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaJoinOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`
- **compare()**: Returns `boolean`

### Public Methods:

- **testExecution()**: Returns `void`
- **compare()**: Returns `boolean`

## 

### Fields:

**Description**: * Test suite for the {@link JavaCollectionSource}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaCountOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaDistinctOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Superclass for tests of {@link JavaExecutionOperator}s.

## 

### Fields:

**Description**: * Test suite for {@link JavaFilterOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaGlobalReduceOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaGlobalReduceOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaJoinOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaLocalCallbackSink}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaReduceByOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaObjectFileSink}.
- **Extends**: JavaExecutionOperatorTestBase
- **testWritingDoesNotFail()**: Returns `void`

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaObjectFileSource}.
- **Extends**: JavaExecutionOperatorTestBase
- **testReading()**: Returns `void`

### Public Methods:

- **testReading()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaRandomSampleOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`
- **testUDFExecution()**: Returns `void`
- **testLargerSampleExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testUDFExecution()**: Returns `void`
- **testLargerSampleExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaReduceByOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaReservoirSampleOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`
- **testLargerSampleExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testLargerSampleExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaSortOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaTextFileSink}.
- **Extends**: JavaExecutionOperatorTestBase
- **defaultLocale**: `Locale` (private)
- **setupTest()**: Returns `void`
- **teardownTest()**: Returns `void`
- **testWritingLocalFile()**: Returns `void`

### Public Methods:

- **setupTest()**: Returns `void`
- **teardownTest()**: Returns `void`
- **testWritingLocalFile()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link JavaUnionAllOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Implementation of a {@link Channel} that is given by a SQL query.
- **Extends**: Channel
- **copy()**: Returns `SqlQueryChannel`
- **getChannel()**: Returns `SqlQueryChannel`
- **setSqlQuery()**: Returns `void`
- **getSqlQuery()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

### Public Methods:

- **copy()**: Returns `SqlQueryChannel`
- **getChannel()**: Returns `SqlQueryChannel`
- **setSqlQuery()**: Returns `void`
- **getSqlQuery()**: Returns `String`
- **equals()**: Returns `boolean`
- **hashCode()**: Returns `int`

## 

### Fields:

**Description**: * This class describes a database.
- **createJdbcConnection()**: Returns `Connection`

### Public Methods:

- **createJdbcConnection()**: Returns `Connection`

## 

### Fields:

**Description**: * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
- **Extends**: ExecutorTemplate
- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **findJdbcExecutionOperatorTaskInStage()**: Returns `ExecutionTask`
- **getSqlClause()**: Returns `String`
- **saveResult()**: Returns `void`

### Public Methods:

- **execute()**: Returns `void`
- **dispose()**: Returns `void`
- **getPlatform()**: Returns `Platform`
- **findJdbcExecutionOperatorTaskInStage()**: Returns `ExecutionTask`
- **getSqlClause()**: Returns `String`
- **saveResult()**: Returns `void`

## 

### Fields:

- **Extends**: ExecutionOperator

## 

### Fields:

- **Extends**: FilterOperator
- **Implements**: JdbcExecutionOperator
- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

### Public Methods:

- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

## 

### Fields:

- **Extends**: MapOperator
- **Implements**: JdbcExecutionOperator
- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

### Public Methods:

- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

## 

### Fields:

- **Extends**: TableSource
- **Implements**: JdbcExecutionOperator
- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getCardinalityEstimator()**: Returns `CardinalityEstimator`
- **estimate()**: Returns `CardinalityEstimate`

### Public Methods:

- **createSqlClause()**: Returns `String`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getCardinalityEstimator()**: Returns `CardinalityEstimator`
- **estimate()**: Returns `CardinalityEstimate`

## 

### Fields:

**Description**: * This {@link Operator} converts {@link SqlQueryChannel}s to {@link StreamChannel}s.
- **Extends**: UnaryToUnaryOperator
- **Implements**: JavaExecutionOperator, JsonSerializable
- **resultSet**: `ResultSet` (private)
- **next**: `Record` (private)
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `Record`
- **close()**: Returns `void`
- **toJson()**: Returns `WayangJsonObj`
- **moveToNext()**: Returns `void`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **hasNext()**: Returns `boolean`
- **next()**: Returns `Record`
- **close()**: Returns `void`
- **toJson()**: Returns `WayangJsonObj`
- **moveToNext()**: Returns `void`

## 

### Fields:

- **Extends**: Platform
- **getConnection()**: Returns `Connection`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **getPlatformId()**: Returns `String`
- **createDatabaseDescriptor()**: Returns `DatabaseDescriptor`
- **getDefaultConfigurationFile()**: Returns `String`

### Public Methods:

- **getConnection()**: Returns `Connection`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **getPlatformId()**: Returns `String`
- **createDatabaseDescriptor()**: Returns `DatabaseDescriptor`
- **getDefaultConfigurationFile()**: Returns `String`

## 

### Fields:

**Description**: * Test suite for {@link JdbcExecutor}.
- **testExecuteWithPlainTableSource()**: Returns `void`
- **testExecuteWithFilter()**: Returns `void`
- **testExecuteWithProjection()**: Returns `void`
- **testExecuteWithProjectionAndFilters()**: Returns `void`

### Public Methods:

- **testExecuteWithPlainTableSource()**: Returns `void`
- **testExecuteWithFilter()**: Returns `void`
- **testExecuteWithProjection()**: Returns `void`
- **testExecuteWithProjectionAndFilters()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SqlToStreamOperator}.
- **testCardinalityEstimator()**: Returns `void`

### Public Methods:

- **testCardinalityEstimator()**: Returns `void`

## 

### Fields:

**Description**: * Test base for {@link JdbcExecutionOperator}s and other {@link ExecutionOperator}s in this module.

## 

### Fields:

**Description**: * Test suite for {@link SqlToStreamOperator}.
- **Extends**: OperatorTestBase
- **testWithHsqldb()**: Returns `void`
- **testWithEmptyHsqldb()**: Returns `void`

### Public Methods:

- **testWithHsqldb()**: Returns `void`
- **testWithEmptyHsqldb()**: Returns `void`

## 

### Fields:

**Description**: * Test implementation of {@link JdbcFilterOperator}.
- **Extends**: JdbcFilterOperator
- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link JdbcPlatformTemplate} implementation based on HSQLDB for test purposes.
- **Extends**: JdbcPlatformTemplate

## 

### Fields:

**Description**: * Test implementation of {@link JdbcFilterOperator}.
- **Extends**: JdbcProjectionOperator
- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Test implementation of {@link JdbcFilterOperator}.
- **Extends**: JdbcTableSource
- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getPlatform()**: Returns `HsqldbPlatform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`
- **createReplacementSubplanFactory()**: Returns `ReplacementSubplanFactory`

## 

### Fields:

- **Extends**: JdbcExecutionOperator

## 

### Fields:

**Description**: * PostgreSQL implementation of the {@link FilterOperator}.
- **Extends**: JdbcFilterOperator
- **Implements**: PostgresExecutionOperator

## 

### Fields:

**Description**: * PostgreSQL implementation of the {@link FilterOperator}.
- **Extends**: JdbcProjectionOperator
- **Implements**: PostgresExecutionOperator

## 

### Fields:

**Description**: * PostgreSQL implementation for the {@link TableSource}.
- **Extends**: JdbcTableSource
- **Implements**: PostgresExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link Platform} implementation for SQLite3.
- **Extends**: JdbcPlatformTemplate
- **getJdbcDriverClassName()**: Returns `String`

### Public Methods:

- **getJdbcDriverClassName()**: Returns `String`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link PostgresPlatform}.
- **Implements**: Plugin
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link PostgresPlatform}.
- **Implements**: Plugin
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

**Description**: * {@link Channel} that represents a broadcasted value.
- **Extends**: Channel
- **copy()**: Returns `BroadcastChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getChannel()**: Returns `BroadcastChannel`

### Public Methods:

- **copy()**: Returns `BroadcastChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getChannel()**: Returns `BroadcastChannel`

## 

### Fields:

**Description**: * Utilities for {@link FileChannel}s.

## 

### Fields:

**Description**: * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
- **Extends**: Channel
- **accumulator**: `LongAccumulator` (private)
- **copy()**: Returns `RddChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **getChannel()**: Returns `RddChannel`
- **isRddCached()**: Returns `boolean`

### Public Methods:

- **copy()**: Returns `RddChannel`
- **createInstance()**: Returns `Instance`
- **accept()**: Returns `void`
- **getMeasuredCardinality()**: Returns `OptionalLong`
- **getChannel()**: Returns `RddChannel`
- **isRddCached()**: Returns `boolean`

## 

### Fields:

**Description**: * Wraps a {@link java.util.function.BinaryOperator} as a {@link Function2}.
- **Implements**: Function2
- **binaryOperator**: `BinaryOperator<Type>` (private)
- **call()**: Returns `Type`

### Public Methods:

- **call()**: Returns `Type`

## 

### Fields:

**Description**: * Implements a {@link Function2} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
- **Implements**: Function2
- **call()**: Returns `Type`

### Public Methods:

- **call()**: Returns `Type`

## 

### Fields:

**Description**: * Implements a {@link FlatMapFunction} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
- **Implements**: FlatMapFunction
- **call()**: Returns `Iterator<OutputType>`

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

## 

### Fields:

**Description**: * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
- **Extends**: Function
- **Implements**: Function
- **call()**: Returns `OutputType`

### Public Methods:

- **call()**: Returns `OutputType`

## 

### Fields:

**Description**: * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
- **Implements**: Function
- **call()**: Returns `OutputType`

### Public Methods:

- **call()**: Returns `OutputType`

## 

### Fields:

**Description**: * Wraps a {@link Function} as a {@link FlatMapFunction}.
- **Implements**: FlatMapFunction
- **call()**: Returns `Iterator<OutputType>`

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

## 

### Fields:

**Description**: * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
- **Implements**: Function
- **call()**: Returns `Boolean`

### Public Methods:

- **call()**: Returns `Boolean`

## 

### Fields:

**Description**: * Wraps a {@link java.util.function.Function} as a {@link FlatMapFunction}.
- **Implements**: FlatMapFunction
- **call()**: Returns `Iterator<OutputType>`

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

## 

### Fields:

**Description**: * Wraps a {@link java.util.function.Function} as a {@link Function}.
- **Implements**: Function
- **call()**: Returns `OutputType`

### Public Methods:

- **call()**: Returns `OutputType`

## 

### Fields:

**Description**: * Wraps a {@link Function} as a {@link FlatMapFunction}.
- **Implements**: FlatMapFunction
- **call()**: Returns `Iterator<OutputType>`

### Public Methods:

- **call()**: Returns `Iterator<OutputType>`

## 

### Fields:

**Description**: * Wraps a {@link Predicate} as a {@link Function}.
- **Implements**: Function
- **predicate**: `Predicate<InputType>` (private)
- **call()**: Returns `Boolean`

### Public Methods:

- **call()**: Returns `Boolean`

## 

### Fields:

**Description**: * Wraps and manages a {@link JavaSparkContext} to avoid steady re-creation.
- **Extends**: ExecutionResourceTemplate
- **get()**: Returns `JavaSparkContext`

### Public Methods:

- **get()**: Returns `JavaSparkContext`

## 

### Fields:

**Description**: * {@link ExecutionContext} implementation for the {@link SparkPlatform}.
- **Implements**: ExecutionContext, Serializable
- **iterationNumber**: `int` (private)
- **getCurrentIteration()**: Returns `int`

### Public Methods:

- **getCurrentIteration()**: Returns `int`

## 

### Fields:

**Description**: * {@link Executor} implementation for the {@link SparkPlatform}.
- **Extends**: PushExecutorTemplate
- **forward()**: Returns `void`
- **getPlatform()**: Returns `SparkPlatform`
- **getNumDefaultPartitions()**: Returns `int`
- **dispose()**: Returns `void`
- **getCompiler()**: Returns `FunctionCompiler`

### Public Methods:

- **forward()**: Returns `void`
- **getPlatform()**: Returns `SparkPlatform`
- **getNumDefaultPartitions()**: Returns `int`
- **dispose()**: Returns `void`
- **getCompiler()**: Returns `FunctionCompiler`

## 

### Fields:

**Description**: * Spark implementation of the {@link SparkBernoulliSampleOperator}.
- **Extends**: SampleOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Takes care of creating a {@link Broadcast} that can be used later on.
- **Extends**: UnaryToUnaryOperator
- **Implements**: SparkExecutionOperator
- **containsAction()**: Returns `boolean`
- **getType()**: Returns `DataSetType<Type>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **containsAction()**: Returns `boolean`
- **getType()**: Returns `DataSetType<Type>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Converts an uncached {@link RddChannel} into a cached {@link RddChannel}.
- **Extends**: UnaryToUnaryOperator
- **Implements**: SparkExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

## 

### Fields:

**Description**: * Spark implementation of the {@link CartesianOperator}.
- **Extends**: CartesianOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link JoinOperator}.
- **Extends**: CoGroupOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Converts a {@link RddChannel} into a {@link CollectionChannel} of the {@link JavaPlatform}.
- **Extends**: UnaryToUnaryOperator
- **Implements**: SparkExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **createCardinalityEstimator()**: Returns `Optional<CardinalityEstimator>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`

## 

### Fields:

**Description**: * Provides a {@link Collection} to a Spark job. Can also be used to convert {@link CollectionChannel}s of the
- **Extends**: CollectionSource
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link CountOperator}.
- **Extends**: CountOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link DistinctOperator}.
- **Extends**: DistinctOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link DoWhileOperator}.
- **Extends**: DoWhileOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

- **Extends**: ExecutionOperator

## 

### Fields:

**Description**: * Spark implementation of the {@link FilterOperator}.
- **Extends**: FilterOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link FlatMapOperator}.
- **Extends**: FlatMapOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link GlobalMaterializedGroupOperator}.
- **Extends**: GlobalMaterializedGroupOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link GlobalReduceOperator}.
- **Extends**: GlobalReduceOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link JoinOperator}.
- **Extends**: IntersectOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link JoinOperator}.
- **Extends**: JoinOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Implementation of the {@link LocalCallbackSink} operator for the Spark platform.
- **Extends**: Serializable
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link LoopOperator}.
- **Extends**: LoopOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link org.apache.wayang.basic.operators.MapOperator}.
- **Extends**: MapOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link MapPartitionsOperator}.
- **Extends**: MapPartitionsOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link MaterializedGroupByOperator}.
- **Extends**: MaterializedGroupByOperator
- **Implements**: SparkExecutionOperator
- **call()**: Returns `Iterable<Type>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **call()**: Returns `Iterable<Type>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
- **Extends**: ObjectFileSink
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
- **Extends**: ObjectFileSource
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link SampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)
- **Extends**: SampleOperator
- **Implements**: SparkExecutionOperator
- **rand**: `Random` (private)
- **start_id**: `int` (private)
- **end_id**: `int` (private)
- **ids**: `ArrayList<Integer>` (private)
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **containsAction()**: Returns `boolean`
- **apply()**: Returns `List<V>`
- **apply()**: Returns `List<V>`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **containsAction()**: Returns `boolean`
- **apply()**: Returns `List<V>`
- **apply()**: Returns `List<V>`

## 

### Fields:

**Description**: * Spark implementation of the {@link ReduceByOperator}.
- **Extends**: ReduceByOperator
- **Implements**: SparkExecutionOperator
- **call()**: Returns `InputType`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **call()**: Returns `InputType`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link RepeatOperator}.
- **Extends**: RepeatOperator
- **Implements**: SparkExecutionOperator
- **iterationCounter**: `int` (private)
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link SparkShufflePartitionSampleOperator}.
- **Extends**: SampleOperator
- **Implements**: SparkExecutionOperator
- **rand**: `Random` (private)
- **partitions**: `List<Integer>` (private)
- **shuffledRDD**: `JavaRDD<Type>` (private)
- **partitionID**: `int` (private)
- **rand**: `Random` (private)
- **start_id**: `int` (private)
- **end_id**: `int` (private)
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **call()**: Returns `Object`
- **apply()**: Returns `List<V>`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **call()**: Returns `Object`
- **apply()**: Returns `List<V>`

## 

### Fields:

**Description**: * Spark implementation of the {@link SortOperator}.
- **Extends**: SortOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Implementation of the {@link TextFileSink} operator for the Spark platform.
- **Extends**: TextFileSink
- **Implements**: SparkExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **createLoadProfileEstimator()**: Returns `Optional<LoadProfileEstimator>`

## 

### Fields:

**Description**: * Provides a {@link Collection} to a Spark job.
- **Extends**: TextFileSource
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKeys()**: Returns `Collection<String>`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a TSV file.
- **Extends**: Tuple2
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
- **Extends**: UnarySource
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link UnionAllOperator}.
- **Extends**: UnionAllOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link MapOperator}.
- **Extends**: ZipWithIdOperator
- **Implements**: SparkExecutionOperator
- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getLoadProfileEstimatorConfigurationKey()**: Returns `String`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * {@link Platform} for Apache Spark.
- **Extends**: Platform
- **sparkContextReference**: `SparkContextReference` (private)
- **getSparkContext()**: Returns `SparkContextReference`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **warmUp()**: Returns `void`
- **getInitializeMillis()**: Returns `long`
- **registerJarIfNotNull()**: Returns `void`

### Public Methods:

- **getSparkContext()**: Returns `SparkContextReference`
- **configureDefaults()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`
- **warmUp()**: Returns `void`
- **getInitializeMillis()**: Returns `long`
- **registerJarIfNotNull()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to create {@link org.apache.spark.rdd.RDD}s.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link JavaPlatform}.
- **Implements**: Plugin
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkBernoulliSampleOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`
- **testDoesNotFail()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testDoesNotFail()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkCartesianOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`
- **compare()**: Returns `boolean`

### Public Methods:

- **testExecution()**: Returns `void`
- **compare()**: Returns `boolean`

## 

### Fields:

**Description**: * Test suite for the {@link SparkCollectionSource}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkCountOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkDistinctOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkFilterOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkFilterOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkGlobalMaterializedGroupOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkGlobalReduceOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithoutData()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkFilterOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkMaterializedGroupByOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkObjectFileSink}.
- **Extends**: SparkOperatorTestBase
- **testWritingDoesNotFail()**: Returns `void`

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkObjectFileSource}.
- **Extends**: SparkOperatorTestBase
- **testWritingDoesNotFail()**: Returns `void`

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

## 

### Fields:

**Description**: * Test base for {@link SparkExecutionOperator} tests.
- **setUp()**: Returns `void`
- **getSC()**: Returns `JavaSparkContext`

### Public Methods:

- **setUp()**: Returns `void`
- **getSC()**: Returns `JavaSparkContext`

## 

### Fields:

**Description**: * Test suite for {@link SparkRandomPartitionSampleOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`
- **testUDFExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testUDFExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkReduceByOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkShufflePartitionSampleOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`
- **testExecutionWithUnknownDatasetSize()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`
- **testExecutionWithUnknownDatasetSize()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkSortOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkTextFileSink}.
- **Extends**: SparkOperatorTestBase
- **testWritingDoesNotFail()**: Returns `void`

### Public Methods:

- **testWritingDoesNotFail()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkUnionAllOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Register for relevant components of this module.

## 

### Fields:

**Description**: * Implementation of the {@link FilterOperator} for the {@link Sqlite3Platform}.
- **Extends**: JdbcFilterOperator
- **getPlatform()**: Returns `Sqlite3Platform`

### Public Methods:

- **getPlatform()**: Returns `Sqlite3Platform`

## 

### Fields:

**Description**: * Implementation of the {@link JdbcProjectionOperator} for the {@link Sqlite3Platform}.
- **Extends**: JdbcProjectionOperator
- **getPlatform()**: Returns `Sqlite3Platform`

### Public Methods:

- **getPlatform()**: Returns `Sqlite3Platform`

## 

### Fields:

**Description**: * Implementation of the {@link TableSource} for the {@link Sqlite3Platform}.
- **Extends**: JdbcTableSource
- **getPlatform()**: Returns `Sqlite3Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getPlatform()**: Returns `Sqlite3Platform`
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * {@link Platform} implementation for SQLite3.
- **Extends**: JdbcPlatformTemplate
- **getJdbcDriverClassName()**: Returns `String`

### Public Methods:

- **getJdbcDriverClassName()**: Returns `String`

## 

### Fields:

**Description**: * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link Sqlite3Platform}.
- **Implements**: Plugin
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

## 

### Fields:

**Description**: * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link Sqlite3Platform}.
- **Implements**: Plugin
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

### Public Methods:

- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getMappings()**: Returns `Collection<Mapping>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`

---

# wayang-plugins

## # wayang-plugins

### Fields:

**Description**: * Provides {@link Plugin}s that enable usage of the {@link IEJoinOperator} and the {@link IESelfJoinOperator}.
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

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable
- **Implements**: Serializable, Comparable
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

## 

### Fields:

**Description**: * {@link Mapping}s for the {@link IEJoinOperator}.

## 

### Fields:

**Description**: * Mapping from {@link IEJoinOperator} to {@link SparkIEJoinOperator}.
- **Extends**: Record
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

## 

### Fields:

**Description**: * Mapping from {@link IESelfJoinOperator} to {@link SparkIESelfJoinOperator}.
- **Extends**: Record
- **Implements**: Mapping
- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

### Public Methods:

- **getTransformations()**: Returns `Collection<PlanTransformation>`
- **createSubplanPattern()**: Returns `SubplanPattern`

## 

### Fields:

**Description**: * This operator decides the correct sorting orders for IEJoin

## 

### Fields:

**Description**: * This operator applies inequality join on elements of input datasets.
- **Extends**: BinaryToUnaryOperator
- **assignSortOrders()**: Returns `void`

### Public Methods:

- **assignSortOrders()**: Returns `void`

## 

### Fields:

**Description**: * This operator applies inequality self join on elements of input datasets.
- **Extends**: Comparable
- **assignSortOrders()**: Returns `void`

### Public Methods:

- **assignSortOrders()**: Returns `void`

## 

### Fields:

**Description**: * Java implementation of the {@link IEJoinOperator}.
- **Extends**: Comparable
- **Implements**: JavaExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Java implementation of the {@link IESelfJoinOperator}.
- **Extends**: Comparable
- **Implements**: JavaExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`

## 

### Fields:

**Description**: * Spark implementation of the {@link   IEJoinOperator}.
- **Extends**: Comparable
- **Implements**: SparkExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Spark implementation of the {@link   IESelfJoinOperator}.
- **Extends**: Comparable
- **Implements**: SparkExecutionOperator
- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

### Public Methods:

- **getSupportedInputChannels()**: Returns `List<ChannelDescriptor>`
- **getSupportedOutputChannels()**: Returns `List<ChannelDescriptor>`
- **containsAction()**: Returns `boolean`

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable
- **Implements**: Serializable, Comparator
- **compare()**: Returns `int`

### Public Methods:

- **compare()**: Returns `int`

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable
- **Implements**: Function
- **call()**: Returns `Data`

### Public Methods:

- **call()**: Returns `Data`

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable

## 

### Fields:

- **Implements**: Serializable, Comparator
- **compare()**: Returns `int`

### Public Methods:

- **compare()**: Returns `int`

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable
- **Implements**: Serializable
- **getHeadTupleValue()**: Returns `Type0`
- **getPartitionID()**: Returns `long`
- **isEmpty()**: Returns `boolean`
- **toString()**: Returns `String`

### Public Methods:

- **getHeadTupleValue()**: Returns `Type0`
- **getPartitionID()**: Returns `long`
- **isEmpty()**: Returns `boolean`
- **toString()**: Returns `String`

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Copyable

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable

## 

### Fields:

**Description**: * Created by khayyzy on 5/28/16.
- **Extends**: Comparable
- **call()**: Returns `Boolean`
- **compareMinMax()**: Returns `boolean`
- **compare()**: Returns `boolean`

### Public Methods:

- **call()**: Returns `Boolean`
- **compareMinMax()**: Returns `boolean`
- **compare()**: Returns `boolean`

## 

### Fields:

**Description**: * Superclass for tests of {@link JavaExecutionOperator}s.

## 

### Fields:

**Description**: * Test suite for {@link JavaIEJoinOperator}.
- **Extends**: JavaExecutionOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkIEJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkIEJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkIEJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkIEJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test suite for {@link SparkIEJoinOperator}.
- **Extends**: SparkOperatorTestBase
- **testExecution()**: Returns `void`

### Public Methods:

- **testExecution()**: Returns `void`

## 

### Fields:

**Description**: * Test base for {@link SparkExecutionOperator} tests.
- **setUp()**: Returns `void`
- **getSC()**: Returns `JavaSparkContext`

### Public Methods:

- **setUp()**: Returns `void`
- **getSC()**: Returns `JavaSparkContext`

## 

### Fields:

**Description**: * Utility to create {@link Channel}s in tests.
- **setUp()**: Returns `void`

### Public Methods:

- **setUp()**: Returns `void`

---

# wayang-profiler

## # wayang-profiler

### Fields:

**Description**: * Utility to create common data generators.
- **Extends**: Supplier

## 

### Fields:

**Description**: * Profiles the reading and writing speed to some directory.
- **profile()**: Returns `String`
- **profileWriting()**: Returns `long`
- **profileReading()**: Returns `long`

### Public Methods:

- **profile()**: Returns `String`
- **profileWriting()**: Returns `long`
- **profileReading()**: Returns `long`

## 

### Fields:

**Description**: * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with two inputs and one output.
- **Extends**: SparkOperatorProfiler

## 

### Fields:

**Description**: * {@link OperatorProfiler} for {@link JavaCollectionSource}s.
- **Extends**: SourceProfiler
- **sourceCollection**: `Collection<Object>` (private)
- **createOperator()**: Returns `JavaCollectionSource`

### Public Methods:

- **createOperator()**: Returns `JavaCollectionSource`

## 

### Fields:

**Description**: * {@link OperatorProfiler} for sources.
- **Extends**: SourceProfiler
- **tempFile**: `File` (private)

## 

### Fields:

- **cpuMhz**: `int` (public)
- **inputCardinalities**: `List<Long>` (private)
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
- **calculateCpuCycles()**: Returns `long`

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
- **calculateCpuCycles()**: Returns `long`

## 

### Fields:

**Description**: * Utilities to create {@link SparkOperatorProfiler} instances.

## 

### Fields:

**Description**: * Utility to support finding reasonable {@link LoadProfileEstimator}s for {@link JavaExecutionOperator}s.

## 

### Fields:

**Description**: * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with one input and no outputs.
- **Extends**: SparkOperatorProfiler

## 

### Fields:

- **Extends**: OperatorProfiler
- **outputChannelInstance**: `JavaChannelInstance` (private)
- **prepare()**: Returns `void`

### Public Methods:

- **prepare()**: Returns `void`

## 

### Fields:

**Description**: * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.
- **Extends**: OperatorProfiler
- **prepare()**: Returns `void`
- **executeOperator()**: Returns `long`
- **getOperator()**: Returns `JavaExecutionOperator`

### Public Methods:

- **prepare()**: Returns `void`
- **executeOperator()**: Returns `long`
- **getOperator()**: Returns `JavaExecutionOperator`

## 

### Fields:

**Description**: * {@link EstimationContext} implementation for {@link DynamicLoadEstimator}s.
- **Implements**: EstimationContext
- **getDoubleProperty()**: Returns `double`
- **getNumExecutions()**: Returns `int`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getIndividual()**: Returns `Individual`

### Public Methods:

- **getDoubleProperty()**: Returns `double`
- **getNumExecutions()**: Returns `int`
- **getPropertyKeys()**: Returns `Collection<String>`
- **getIndividual()**: Returns `Individual`

## 

### Fields:

**Description**: * Adjustable {@link LoadProfileEstimator} implementation.
- **Extends**: LoadEstimator
- **getVariable()**: Returns `double`
- **calculate()**: Returns `LoadEstimate`
- **toMathEx()**: Returns `String`
- **getEmployedVariables()**: Returns `Collection<Variable>`

### Public Methods:

- **getVariable()**: Returns `double`
- **calculate()**: Returns `LoadEstimate`
- **toMathEx()**: Returns `String`
- **getEmployedVariables()**: Returns `Collection<Variable>`

## 

### Fields:

**Description**: * Adjustable {@link LoadProfileEstimator} implementation.
- **Implements**: LoadProfileEstimator
- **estimate()**: Returns `LoadProfile`
- **nest()**: Returns `void`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **toJsonConfig()**: Returns `String`
- **getEmployedVariables()**: Returns `Collection<Variable>`

### Public Methods:

- **estimate()**: Returns `LoadProfile`
- **nest()**: Returns `void`
- **getNestedEstimators()**: Returns `Collection<LoadProfileEstimator>`
- **getConfigurationKey()**: Returns `String`
- **toJsonConfig()**: Returns `String`
- **getEmployedVariables()**: Returns `Collection<Variable>`

## 

### Fields:

**Description**: * Utility to create {@link DynamicLoadProfileEstimator}s.
- **estimate()**: Returns `LoadProfile`
- **getEmployedVariables()**: Returns `Collection<Variable>`
- **calculate()**: Returns `LoadEstimate`

### Public Methods:

- **estimate()**: Returns `LoadProfile`
- **getEmployedVariables()**: Returns `Collection<Variable>`
- **calculate()**: Returns `LoadEstimate`

## 

### Fields:

**Description**: * Implementation of the genetic optimization technique for finding good {@link LoadProfileEstimator}s.
- **runtimeSum**: `long` (private)
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
- **adjustOrPutValue()**: Returns `void`
- **updateFitnessOf()**: Returns `void`

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
- **adjustOrPutValue()**: Returns `void`
- **updateFitnessOf()**: Returns `void`

## 

### Fields:

**Description**: * This app tries to infer good {@link LoadProfileEstimator}s for {@link ExecutionOperator}s using data from an
- **run()**: Returns `void`
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

### Public Methods:

- **run()**: Returns `void`
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

## 

### Fields:

**Description**: * Context for the optimization of {@link LoadProfileEstimator}s.
- **fitnessAccumulator**: `double` (private)
- **weightAccumulator**: `double` (private)
- **setGene()**: Returns `void`
- **mutate()**: Returns `Individual`
- **crossOver()**: Returns `Individual`
- **updateMaturity()**: Returns `void`
- **updateFitness()**: Returns `double`
- **getFitness()**: Returns `double`
- **updateMaturity()**: Returns `void`
- **calculateRelativeDelta()**: Returns `double`
- **calculateAbsolutePartialFitness()**: Returns `double`
- **estimateTime()**: Returns `TimeEstimate`

### Public Methods:

- **setGene()**: Returns `void`
- **mutate()**: Returns `Individual`
- **crossOver()**: Returns `Individual`
- **updateMaturity()**: Returns `void`
- **updateFitness()**: Returns `double`
- **getFitness()**: Returns `double`
- **updateMaturity()**: Returns `void`
- **calculateRelativeDelta()**: Returns `double`
- **calculateAbsolutePartialFitness()**: Returns `double`
- **estimateTime()**: Returns `TimeEstimate`

## 

### Fields:

**Description**: * Evaluates a {@link Configuration} on a {@link ExecutionLog}.
- **sortCriterion**: `Comparator<PartialExecution>` (private)
- **runUserLoop()**: Returns `void`
- **printPartialExecutions()**: Returns `void`
- **print()**: Returns `void`
- **printStatistics()**: Returns `void`
- **modifyFilters()**: Returns `void`
- **modifySorting()**: Returns `void`
- **createPartialExecutionStream()**: Returns `Stream<PartialExecution>`

### Public Methods:

- **runUserLoop()**: Returns `void`
- **printPartialExecutions()**: Returns `void`
- **print()**: Returns `void`
- **printStatistics()**: Returns `void`
- **modifyFilters()**: Returns `void`
- **modifySorting()**: Returns `void`
- **createPartialExecutionStream()**: Returns `Stream<PartialExecution>`

## 

### Fields:

**Description**: * Context for the optimization of {@link LoadProfileEstimator}s.
- **getOrCreateVariable()**: Returns `Variable`
- **getVariable()**: Returns `Variable`
- **getVariable()**: Returns `Variable`
- **createRandomIndividual()**: Returns `Individual`
- **getVariables()**: Returns `List<Variable>`
- **getNumDimensions()**: Returns `int`

### Public Methods:

- **getOrCreateVariable()**: Returns `Variable`
- **getVariable()**: Returns `Variable`
- **getVariable()**: Returns `Variable`
- **createRandomIndividual()**: Returns `Individual`
- **getVariables()**: Returns `List<Variable>`
- **getNumDimensions()**: Returns `int`

## 

### Fields:

**Description**: * A variable that can be altered by an optimization algorithm.
- **getId()**: Returns `String`
- **getValue()**: Returns `double`
- **createRandomValue()**: Returns `double`
- **mutate()**: Returns `double`
- **setRandomValue()**: Returns `void`
- **getIndex()**: Returns `int`
- **toString()**: Returns `String`

### Public Methods:

- **getId()**: Returns `String`
- **getValue()**: Returns `double`
- **createRandomValue()**: Returns `double`
- **mutate()**: Returns `double`
- **setRandomValue()**: Returns `void`
- **getIndex()**: Returns `int`
- **toString()**: Returns `String`

## 

## 

### Fields:

- **Implements**: Sampler
- **sample()**: Returns `List<T>`

### Public Methods:

- **sample()**: Returns `List<T>`

## 

## 

### Fields:

**Description**: * Sampling strategy that simulates a tournament between elements.
- **Implements**: Sampler
- **sample()**: Returns `List<T>`

### Public Methods:

- **sample()**: Returns `List<T>`

## 

### Fields:

**Description**: * Starts a profiling run of Spark.

## 

### Fields:

**Description**: * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
- **Extends**: SparkSourceProfiler
- **cleanUp()**: Returns `void`

### Public Methods:

- **cleanUp()**: Returns `void`

## 

### Fields:

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
- **waitAndQueryMetricAverage()**: Returns `double`

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
- **waitAndQueryMetricAverage()**: Returns `double`

## 

### Fields:

- **Extends**: SparkOperatorProfiler

## 

### Fields:

**Description**: * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
- **Extends**: SparkSourceProfiler
- **cleanUp()**: Returns `void`

### Public Methods:

- **cleanUp()**: Returns `void`

## 

### Fields:

**Description**: * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with one input and one output.
- **Extends**: SparkOperatorProfiler

## 

### Fields:

**Description**: * Utilities to fake Wayang internals etc..

## 

### Fields:

**Description**: * Utility to read from an RRD file.
- **Implements**: AutoCloseable
- **query()**: Returns `double`
- **getLastUpdateMillis()**: Returns `long`
- **close()**: Returns `void`

### Public Methods:

- **query()**: Returns `double`
- **getLastUpdateMillis()**: Returns `long`
- **close()**: Returns `void`

---

# wayang-resources

---

# wayang-tests-integration

## # wayang-tests-integration

### Fields:

**Description**: * Test the Spark integration with Wayang.
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
- **makeContext()**: Returns `WayangContext`
- **makeAndRun()**: Returns `void`
- **makeList()**: Returns `List<String>`

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
- **makeContext()**: Returns `WayangContext`
- **makeAndRun()**: Returns `void`
- **makeList()**: Returns `List<String>`

## 

### Fields:

**Description**: * Test the Java integration with Wayang.
- **configuration**: `Configuration` (private)
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

## 

### Fields:

**Description**: * Integration tests for the integration of Giraph with Wayang.
- **testPageRankWithJava()**: Returns `void`
- **testPageRankWithoutGiraph()**: Returns `void`
- **check()**: Returns `void`

### Public Methods:

- **testPageRankWithJava()**: Returns `void`
- **testPageRankWithoutGiraph()**: Returns `void`
- **check()**: Returns `void`

## 

### Fields:

**Description**: * Test the Java integration with Wayang.
- **allowedInts**: `Set<Integer>` (private)
- **coefficient**: `int` (private)
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

## 

## 

### Fields:

**Description**: * This class hosts and documents some tests for bugs that we encountered. Ultimately, we want to avoid re-introducing
- **testCollectionToRddAndBroadcast()**: Returns `void`

### Public Methods:

- **testCollectionToRddAndBroadcast()**: Returns `void`

## 

### Fields:

**Description**: * Test the Spark integration with Wayang.
- **Implements**: PredicateDescriptor
- **allowedInts**: `Set<Integer>` (private)
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

## 

### Fields:

**Description**: * Provides plans that can be used for integration testing.
- **Implements**: FunctionDescriptor
- **increment**: `int` (private)
- **open()**: Returns `void`
- **apply()**: Returns `Integer`
- **open()**: Returns `void`
- **open()**: Returns `void`

### Public Methods:

- **open()**: Returns `void`
- **apply()**: Returns `Integer`
- **open()**: Returns `void`
- **open()**: Returns `void`

## 

### Fields:

**Description**: * Provides plans that can be used for integration testing..
- **Extends**: WayangPlans

## 

### Fields:

**Description**: * Word count integration test. Besides going through different {@link Platform} combinations, each test addresses a different
- **testOnJava()**: Returns `void`
- **testOnSpark()**: Returns `void`
- **testOnSparkToJava()**: Returns `void`
- **testOnJavaToSpark()**: Returns `void`
- **testOnJavaAndSpark()**: Returns `void`

### Public Methods:

- **testOnJava()**: Returns `void`
- **testOnSpark()**: Returns `void`
- **testOnSparkToJava()**: Returns `void`
- **testOnJavaToSpark()**: Returns `void`
- **testOnJavaAndSpark()**: Returns `void`

## 

### Fields:

**Description**: * Dummy {@link Platform} that does not provide any {@link Mapping}s.
- **Extends**: Platform
- **Implements**: Plugin
- **configureDefaults()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

### Public Methods:

- **configureDefaults()**: Returns `void`
- **getMappings()**: Returns `Collection<Mapping>`
- **getRequiredPlatforms()**: Returns `Collection<Platform>`
- **getChannelConversions()**: Returns `Collection<ChannelConversion>`
- **setProperties()**: Returns `void`
- **createLoadProfileToTimeConverter()**: Returns `LoadProfileToTimeConverter`
- **createTimeToCostConverter()**: Returns `TimeToCostConverter`

## 

### Fields:

**Description**: * Integration tests for the integration of GraphChi with Wayang.
- **testPageRankWithJava()**: Returns `void`
- **testPageRankWithSpark()**: Returns `void`
- **testPageRankWithoutGraphChi()**: Returns `void`
- **check()**: Returns `void`

### Public Methods:

- **testPageRankWithJava()**: Returns `void`
- **testPageRankWithSpark()**: Returns `void`
- **testPageRankWithoutGraphChi()**: Returns `void`
- **check()**: Returns `void`

---
