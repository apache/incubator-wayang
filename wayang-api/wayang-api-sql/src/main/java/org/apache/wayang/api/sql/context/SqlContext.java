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
import org.apache.wayang.api.utils.Parameters;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;
import org.apache.commons.cli.*;

import com.google.common.io.Resources;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.collection.JavaConversions;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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

        //Specify the named arguments
        Options options = new Options();
        options.addOption("p", "platforms", true, "[platforms...]");
        options.addOption("s", "schema", true, "Schema path");
        options.addOption("q", "query", true, "SQL statement path");
        options.addOption("o", "outputPath", true, "Output path");
        options.addOption("d", "data", true, "Data path for file-based schema");
        options.addOption("c", "config", true, "File path for config file");
        options.addOption("jdbcDriver", true, "JDBC driver");
        options.addOption("jdbcUrl", true, "JDBC URL");
        options.addOption("jdbcPassword", true, "JDBC URL");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        final String queryPath = cmd.getOptionValue("q");
        final String jdbcDriver = cmd.getOptionValue("jdbcDriver");
        final String jdbcUrl = cmd.getOptionValue("jdbcUrl");
        final String jdbcUser = cmd.getOptionValue("jdbcUser");
        final String jdbcPassword = cmd.getOptionValue("jdbcPassword");
        final String outputPath = cmd.getOptionValue("o");
        final String dataPath = cmd.getOptionValue("d");
        final String schemaPath = cmd.getOptionValue("s");

        final String query = StringUtils.chop(
                Files.readString(Paths.get(queryPath))
                        .stripTrailing());

        final String driverPlatform = jdbcDriver.split("\\.")[0];

        final Configuration configuration = new Configuration();

        if (cmd.hasOption("c")) {
            configuration.load(cmd.getOptionValue("c"));
        }

        final String calciteModel = Resources.toString(
                new URL(schemaPath),
                Charset.defaultCharset()
        );

        configuration.setProperty("wayang.calcite.model", calciteModel);
        configuration.setProperty(String.format("wayang.%s.jdbc.url", driverPlatform), jdbcUrl);
        configuration.setProperty(String.format("wayang.%s.jdbc.user", driverPlatform), jdbcUser);
        configuration.setProperty(String.format("wayang.%s.jdbc.password", driverPlatform), jdbcPassword);

        final JSONObject calciteModelJSON = (JSONObject) new JSONParser().parse(calciteModel);

        final Configuration parseModel = new ModelParser(configuration, calciteModelJSON).setProperties();

        final SqlContext context = new SqlContext(parseModel,
                List.of(Java.channelConversionPlugin(), Postgres.conversionPlugin()));

        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(cmd.getOptionValue("p")));
        plugins.stream().forEach(context::register);

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
