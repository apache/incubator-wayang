/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.converter.functions.FilterPredicateImpl;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.schema.WayangSchema;
import org.apache.wayang.api.sql.calcite.schema.WayangSchemaBuilder;
import org.apache.wayang.api.sql.calcite.schema.WayangTable;
import org.apache.wayang.api.sql.calcite.schema.WayangTableBuilder;
import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.basic.data.Record;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SqlToWayangRelTest {

    /**
     * Method for building {@link WayangPlan}s useful for testing, benchmarking and
     * other
     * usages where you want to handle the intermediate {@link WayangPlan}
     * 
     * @param sql     sql query string with the {@code ;} cut off
     * @param udfJars
     * @return a {@link WayangPlan} of a given sql string
     * @throws SqlParseException
     * @throws SQLException
     */
    public Tuple2<Collection<Record>, WayangPlan> buildCollectorAndWayangPlan(final SqlContext context,
            final String sql, final String... udfJars) throws SqlParseException, SQLException {
        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        final Optimizer optimizer = Optimizer.create(
                SchemaUtils.getSchema(context.getConfiguration()),
                configProperties,
                relDataTypeFactory);

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        final RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_INTO_JOIN,
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE,
                WayangRules.WAYANG_SORT_RULE);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        final Collection<Record> collector = new ArrayList<>();

        final WayangPlan wayangPlan = optimizer.convertWithConfig(wayangRel, context.getConfiguration(),
                collector);

        return new Tuple2<>(collector, wayangPlan);
    }

    @Test
    public void javaJoinTest() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/largeLeftTableIndex.csv");
        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex JOIN fs.exampleRefToRef ON largeLeftTableIndex.NAMEA = exampleRefToRef.NAMEA");
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        // except reduce by
        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);

        assert (result.stream()
                .anyMatch(rec -> rec.equals(new Record("test1", "test1", "test2", "test1", "test1"))));
    }

    @Test
    public void javaMultiConditionJoin() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/largeLeftTableIndex.csv");
        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex JOIN fs.exampleRefToRef ON largeLeftTableIndex.NAMEB = exampleRefToRef.NAMEB AND largeLeftTableIndex.NAMEC = exampleRefToRef.NAMEB");
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        // except reduce by
        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);

        final boolean checkEq = result.stream()
                .allMatch(rec -> rec.equals(new Record("", "test2", "test2", "", "test2")));

        assert (checkEq);
    }

    @Test
    public void aggregateCountInJavaWithIntegers() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/exampleInt.csv");
        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT exampleInt.NAMEC, COUNT(*) FROM fs.exampleInt GROUP BY NAMEC");
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        // except reduce by
        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);

        final Record rec = result.stream().findFirst().get();
        assert (rec.size() == 2);
        assert (rec.getInt(1) == 3);
    }

    @Test
    public void aggregateCountInJava() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/largeLeftTableIndex.csv");
        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT largeLeftTableIndex.NAMEC, COUNT(*) FROM fs.largeLeftTableIndex GROUP BY NAMEC");
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        // except reduce by
        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);

        final Record rec = result.stream().findFirst().get();
        assert (rec.size() == 2);
        assert (rec.getInt(1) == 3);
    }

    @Test
    public void filterIsNull() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA IS NULL)" //
        );
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);
        assert (result.size() == 0);
    }

    @Test
    public void javaAverage() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/exampleSort.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT AVG(col1) FROM fs.exampleSort" //
        );
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        sqlContext.execute(wayangPlan);

        assert (result.size() == 1);
        assert (result.stream().findFirst().get().getDouble(0) == 0.875f);
    }

    @Test
    public void filterNotEqualsValue() throws Exception {
        final SqlContext sqlContext = this.createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA <> 'test1')" //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        sqlContext.execute(wayangPlan);

        assert (!result.stream().anyMatch(record -> record.getField(0).equals("test1")));
    }

    @Test
    public void filterIsNotNull() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA IS NOT NULL)" //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        assert (!result.stream().anyMatch(record -> record.getField(0).equals(null)));
    }

    @Test
    public void javaReduceBy() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(
                sqlContext,
                "select exampleSmallA.COLA, count(*) from fs.exampleSmallA group by exampleSmallA.COLA");

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);

        assert (result.stream().anyMatch(rec -> rec.equals(new Record("item1", 2))));
    }

    @Test
    public void javaCrossJoin() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(
                sqlContext,
                "select * from fs.exampleSmallA cross join fs.exampleSmallB");

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        sqlContext.execute(wayangPlan);

        final List<Record> shouldBe = List.of(
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "x", "x", "x"),
                new Record("item1", "item2", "x", "x", "x"));

        final Map<Record, Integer> resultTally = result.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));
        final Map<Record, Integer> shouldBeTally = shouldBe.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));

        assert (resultTally.equals(shouldBeTally));
    }

    @Test
    public void filterWithNotLike() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA NOT LIKE '_est1')" //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        assert (!result.stream().anyMatch(record -> record.getString(0).equals("test1")));
    }

    @Test
    public void filterWithLike() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex WHERE largeLeftTableIndex.NAMEA LIKE '_est1'" //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        assert (result.stream().anyMatch(rec -> rec.equals(new Record("test1", "test1", "test2"))));
    }

    @Test
    public void javaLimit() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/exampleSort.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT col1, col2, col3, count(*) as total from fs.exampleSort group by col1, col2, col3 order by col1 desc, col2, col3 desc LIMIT 1");

        final Collection<Record> r = t.field0;
        final WayangPlan wayangPlan = t.field1;

        sqlContext.execute(wayangPlan);

        final List<Record> result = r.stream().collect(Collectors.toList());

        assert (result.get(0).equals(new Record(2, "a", "a", 2)));
    }

    @Test
    public void javaSort() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/exampleSort.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT col1, col2, col3, count(*) as total from fs.exampleSort group by col1, col2, col3 order by col1 desc, col2, col3 desc");

        final Collection<Record> r = t.field0;
        final WayangPlan wayangPlan = t.field1;

        sqlContext.execute(wayangPlan);

        final List<Record> result = r.stream().collect(Collectors.toList());

        assert (result.get(0).equals(new Record(2, "a", "a", 2)));
        assert (result.get(1).equals(new Record(1, "a", "b", 1)));
        assert (result.get(2).equals(new Record(1, "a", "a", 1)));
        assert (result.get(3).equals(new Record(1, "b", "b", 1)));
        assert (result.get(4).equals(new Record(0, "a", "b", 1)));
        assert (result.get(5).equals(new Record(0, "a", "a", 1)));
        assert (result.get(6).equals(new Record(0, "b", "b", 1)));
    }

    @Test
    public void joinWithLargeLeftTableIndexCorrect() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex AS na INNER JOIN fs.largeLeftTableIndex AS nb ON na.NAMEB = nb.NAMEA " //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        final List<Record> shouldBe = List.of(
                new Record("test1", "test1", "test2", "test1", "test1", "test2"),
                new Record("test2", "", "test2", "", "test2", "test2"),
                new Record("", "test2", "test2", "test2", "", "test2"));

        final Map<Record, Integer> resultTally = result.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));
        final Map<Record, Integer> shouldBeTally = shouldBe.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));

        assert (resultTally.equals(shouldBeTally));
    }

    // Imagine case: l = {item1, item2}, r = {item3,item4}, j = {item1, item2,
    // item3, item4} join on =($1,$3) would be =(item2, item4) in the join set
    // however from the r set we need to factor in the
    // offset, $3 -> 3 - l.size() = $1, r($1) = "item4" we cannot naively assume
    // that it is always ordered as =(lRef,rRef), lRef < rRef.
    // it may also be =($3,$1)
    @Test
    public void joinWithLargeLeftTableIndexMirrorAlias() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex AS na INNER JOIN fs.largeLeftTableIndex AS nb ON nb.NAMEB = na.NAMEA " //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        final List<Record> shouldBe = List.of(
                new Record("test1", "test1", "test2", "test1", "test1", "test2"),
                new Record("test2", "", "test2", "", "test2", "test2"),
                new Record("", "test2", "test2", "test2", "", "test2"));

        final Map<Record, Integer> resultTally = result.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));
        final Map<Record, Integer> shouldBeTally = shouldBe.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));

        assert (resultTally.equals(shouldBeTally));
    }

    // @Test
    public void sparkFilter() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex AS na WHERE na.NAMEA = 'test1'" //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Spark.platform());
        });

        sqlContext.execute(wayangPlan);

        assert (result.stream().anyMatch(rec -> rec.equals(new Record("test1", "test1"))));
    }

    // tests sql-apis ability to serialize projections and joins
    @Test
    public void sparkInnerJoin() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.largeLeftTableIndex AS na INNER JOIN fs.largeLeftTableIndex AS nb ON nb.NAMEB = na.NAMEA " //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Spark.platform());
        });

        sqlContext.execute(wayangPlan);

        final List<Record> shouldBe = List.of(
                new Record("test1", "test1", "test2", "test1", "test1", "test2"),
                new Record("test2", "", "test2", "", "test2", "test2"),
                new Record("", "test2", "test2", "test2", "", "test2"));

        final Map<Record, Integer> resultTally = result.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));
        final Map<Record, Integer> shouldBeTally = shouldBe.stream()
                .collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));

        assert (resultTally.equals(shouldBeTally));
    }

    // @Test
    public void rexSerializationTest() throws Exception {
        // create filterPredicateImpl for serialisation
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final RexBuilder rb = new RexBuilder(typeFactory);
        final RexNode leftOperand = rb.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        final RexNode rightOperand = rb.makeLiteral("test");
        final RexNode cond = rb.makeCall(SqlStdOperatorTable.EQUALS, leftOperand, rightOperand);
        final SerializablePredicate<?> fpImpl = new FilterPredicateImpl(cond);

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(fpImpl);
        objectOutputStream.close();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                byteArrayOutputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        final Object deserializedObject = objectInputStream.readObject();
        objectInputStream.close();

        assert (((FilterPredicateImpl) deserializedObject).test(new Record("test")));
    }

    @Test
    public void exampleFilterTableRefToTableRef() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/exampleRefToRef.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT * FROM fs.exampleRefToRef WHERE exampleRefToRef.NAMEA = exampleRefToRef.NAMEB" //
        );

        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        assert (result.stream().anyMatch(rec -> rec.equals(new Record("test1", "test1"))));
    }

    @Test
    public void exampleMinWithStrings() throws Exception {
        final SqlContext sqlContext = createSqlContext("/data/exampleMin.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = this.buildCollectorAndWayangPlan(sqlContext,
                "SELECT MIN(exampleMin.NAME) FROM fs.exampleMin" //
        );
        final Collection<Record> result = t.field0;
        final WayangPlan wayangPlan = t.field1;
        sqlContext.execute(wayangPlan);

        assert (result.stream().findAny().get().getString(0).equals("AA"));
    }

    public void test_simple_sql() throws Exception {
        final WayangTable customer = WayangTableBuilder.build("customer")
                .addField("id", SqlTypeName.INTEGER)
                .addField("name", SqlTypeName.VARCHAR)
                .addField("age", SqlTypeName.INTEGER)
                .withRowCount(100)
                .build();

        final WayangTable orders = WayangTableBuilder.build("orders")
                .addField("id", SqlTypeName.INTEGER)
                .addField("cid", SqlTypeName.INTEGER)
                .addField("price", SqlTypeName.DECIMAL)
                .addField("quantity", SqlTypeName.INTEGER)
                .withRowCount(100)
                .build();

        final WayangSchema wayangSchema = WayangSchemaBuilder.build("exSchema")
                .addTable(customer)
                .addTable(orders)
                .build();

        final Optimizer optimizer = Optimizer.create(wayangSchema);

        // String sql = "select c.name, c.age from customer c where (c.age < 40 or c.age
        // > 60) and \'alex\' = c.name";
        // String sql = "select c.age from customer c";
        final String sql = "select c.name, c.age, o.price from customer c join orders o on c.id = o.cid where c.age > 40 "
                +
                "and o" +
                ".price < 100";

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        print("After parsing", relNode);

        final RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        print("After rel to wayang conversion", wayangRel);

        // WayangPlan plan = optimizer.convert(wayangRel);

        // print("After Translating to WayangPlan", plan);

    }

    private SqlContext createSqlContext(final String tableResourceName)
            throws IOException, ParseException, SQLException {
        final String calciteModel = "{\r\n" + //
                "    \"calcite\": {\r\n" + //
                "      \"version\": \"1.0\",\r\n" + //
                "      \"defaultSchema\": \"wayang\",\r\n" + //
                "      \"schemas\": [\r\n" + //
                "        {\r\n" + //
                "          \"name\": \"fs\",\r\n" + //
                "          \"type\": \"custom\",\r\n" + //
                "          \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\r\n" + //
                "          \"operand\": {\r\n" + //
                "            \"directory\": \"" + "/" + this.getClass().getResource("/data").getPath()
                + "\"\r\n" + //
                "          }\r\n" + //
                "        }\r\n" + //
                "      ]\r\n" + //
                "    },\r\n" + //
                "    \"separator\": \";\"\r\n" + //
                "  }\r\n" + //
                "  \r\n" + //
                "  \r\n" + //
                "";

        final JSONObject calciteModelJSON = (JSONObject) new JSONParser().parse(calciteModel);
        final Configuration configuration = new ModelParser(new Configuration(), calciteModelJSON)
                .setProperties();
        assert (configuration != null)
                : "Could not get configuration with calcite model: " + calciteModel;

        final String dataPath = this.getClass().getResource(tableResourceName).getPath();
        assert (dataPath != null && dataPath != "")
                : "Could not get table resource from path: " + tableResourceName;

        configuration.setProperty("wayang.fs.table.url", dataPath);

        configuration.setProperty(
                "wayang.ml.executions.file",
                "mle" + ".txt");

        configuration.setProperty(
                "wayang.ml.optimizations.file",
                "mlo" + ".txt");

        configuration.setProperty("wayang.ml.experience.enabled", "false");

        return new SqlContext(configuration);
    }

    private void print(final String header, final WayangPlan plan) {
        final StringWriter sw = new StringWriter();
        sw.append(header).append(":").append("\n");

        final Collection<Operator> operators = PlanTraversal.upstream().traverse(plan.getSinks())
                .getTraversedNodes();
        operators.forEach(o -> sw.append(o.toString()));

        System.out.println(sw.toString());
    }

    private void print(final String header, final RelNode relTree) {
        final StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        final RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES,
                true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());
    }

}
