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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.configuration.KeyValueProvider;
import org.apache.wayang.core.api.configuration.MapBasedKeyValueProvider;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlContext {

    private static final AtomicInteger jobId = new AtomicInteger(0);

    private final Configuration configuration;

    private final WayangContext wayangContext;

    private final CalciteSchema calciteSchema;

    public SqlContext() throws SQLException {
        this(new Configuration());
    }

    public SqlContext(Configuration configuration) throws SQLException {
        this.configuration = configuration.fork(String.format("SqlContext(%s)", configuration.getName()));

        wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        /** hard coded for now **/
        calciteSchema = SchemaUtils.getPostgresSchema(configuration);
//        calciteSchema = SchemaUtils.getFileSchema(configuration);

    }

    public Collection<Record> executeSql(String sql) throws Exception {

        Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        Optimizer optimizer = Optimizer.create(calciteSchema, configProperties,
                relDataTypeFactory);

        SqlNode sqlNode = optimizer.parseSql(sql);
        SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        RelNode relNode = optimizer.convert(validatedSqlNode);

        PrintUtils.print("After pasrsing sql query", relNode);


        RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE
        );
        RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules
        );

        PrintUtils.print("After translating logical intermediate plan", wayangRel);


        Collection<Record> collector = new ArrayList<>();
        WayangPlan wayangPlan = optimizer.convert(wayangRel, collector);
        wayangContext.execute(getJobName(), wayangPlan);

        return collector;
    }

    private static String getJobName() {
        return "SQL["+jobId.incrementAndGet()+"]";
    }


}
