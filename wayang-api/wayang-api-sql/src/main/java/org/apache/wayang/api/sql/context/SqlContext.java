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

package org.apache.wayang.api.sql.context;

import org.apache.commons.lang3.StringUtils;


import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

public class SqlContext extends WayangContext {

    private static final AtomicInteger jobId = new AtomicInteger(0);

    private final CalciteSchema calciteSchema;

    public SqlContext() throws SQLException {
        this(new Configuration());
    }

    public SqlContext(final Configuration configuration) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        this.withPlugin(Java.basicPlugin());
        this.withPlugin(Spark.basicPlugin());
        this.withPlugin(Postgres.plugin());

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    public SqlContext(final Configuration configuration, final List<Plugin> plugins) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        for (final Plugin plugin : plugins) {
            this.withPlugin(plugin);
        }

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    /**
     * Entry point for executing SQL statements while providing arguments.
     * You need to provide at least a JDBC source.
     *
     * @param args args[0] = SQL statement path, args[1] = JDBC driver, args[2] =
     *             JDBC URL, args[3] = JDBC user,
     *                          args[4] = JDBC password, args[5] = outputPath,
     *             args[6...] = platforms
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 5)
            throw new IllegalArgumentException(
                    "Usage: ./bin/wayang-submit org.apache.wayang.api.sql.SqlContext <SQL statement path> <JDBC driver> <JDBC URL> <JDBC user> <JDBC password> <Result output path> [platforms...]");

        final String queryPath = args[0];
        final String jdbcDriver = args[1];
        final String jdbcUrl = args[2];
        final String jdbcUser = args[3];
        final String jdbcPassword = args[4];
        final String outputPath = args[5];

        final String query = StringUtils.chop(
                Files.readString(Paths.get(queryPath))
                        .stripTrailing());

        final String driverPlatform = jdbcDriver.split("\\.")[0];

        final String calciteModel = String.format(
                "{\r\n" +
                        "\"calcite\": {\r\n" +
                        "    \"version\": \"1.0\",\n" +
                        "    \"defaultSchema\": \"wayang\",\n" +
                        "    \"schemas\": [\n" +
                        "        {\n" +
                        "            \"name\": \"postgres\",\n" +
                        "            \"type\": \"custom\",\n" +
                        "            \"factory\": \"org.apache.wayang.api.sql.calcite.jdbc.JdbcSchema$Factory\",\n" +
                        "            \"operand\": {\n" +
                        "                \"jdbcDriver\": \"%s\",\n" +
                        "                \"jdbcUrl\": \"%s\",\n" +
                        "                \"jdbcUser\": \"%s\",\n" +
                        "                \"jdbcPassword\": \"%s\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    ]\n" +
                        "}\r\n" +
                        "}",
                jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword);

        final Configuration configuration = new Configuration();
        configuration.load(ReflectionUtils.loadResource("wayang-defaults.properties"));

        configuration.setProperty("wayang.calcite.model", calciteModel);
        configuration.setProperty(String.format("wayang.%s.jdbc.url", driverPlatform), jdbcUrl);
        configuration.setProperty(String.format("wayang.%s.jdbc.user", driverPlatform), jdbcUser);
        configuration.setProperty(String.format("wayang.%s.jdbc.password", driverPlatform), jdbcPassword);

        final JSONObject calciteModelJSON = (JSONObject) new JSONParser().parse(calciteModel);

        final Configuration parseModel = new ModelParser(configuration, calciteModelJSON).setProperties();

        final SqlContext context = new SqlContext(parseModel,
                List.of(Java.channelConversionPlugin(), Postgres.conversionPlugin()));

        for (int i = 6; i < args.length; i++) {
            final String platform = args[i];

            switch (platform.toLowerCase()) {
                case "spark":
                    context.withPlugin(Spark.basicPlugin());
                    break;
                case "java":
                    context.withPlugin(Java.basicPlugin());
                    break;
                case "postgres":
                    context.withPlugin(Postgres.plugin());
                    break;
                default:
                    throw new IllegalArgumentException("platform not supported " + platform);
            }
        }

        final Collection<Record> result = context.executeSql(query);

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputPath))) {
            for (final Record record : result) {
                writer.write(record.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Collection<Record> executeSql(final String sql) throws SqlParseException {

        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        final Optimizer optimizer = Optimizer.create(calciteSchema, configProperties,
                relDataTypeFactory);

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        PrintUtils.print("After parsing sql query", relNode);

        final RuleSet rules = RuleSets.ofList(
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

        PrintUtils.print("After translating logical intermediate plan", wayangRel);

        final Collection<Record> collector = new ArrayList<>();
        final WayangPlan wayangPlan = optimizer.convert(wayangRel, collector);
        this.execute(getJobName(), wayangPlan);

        return collector;
    }

    private static String getJobName() {
        return "SQL[" + jobId.incrementAndGet() + "]";
    }

}
