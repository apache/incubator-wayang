/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.wayang.api.sql.parser;

import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.wayang.api.sql.relation.node.WayangRel;
import org.apache.wayang.api.sql.relation.node.WayangRelFactories;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

public class Main {

  public static void main(String... args) throws IOException, SQLException {
    CalciteConnection connection = new SimpleCalciteConnection();
    String salesSchema = Resources.toString(Main.class.getResource("/WayangModel.json"), Charset.defaultCharset());
    // ModelHandler reads the sales schema and load the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + salesSchema);

    // Create the query planner with sales schema. conneciton.getSchema returns default schema name specified in sales.json
    SchemaPlus schema = connection.getRootSchema().getSubSchema(connection.getSchema());


    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);


    FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
            // case when they are read, and whether identifiers are matched case-sensitively.
            .setLex(Lex.MYSQL)
            .build())
        // Sets the schema to use by the planner
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        // Context provides a way to store data within the planner session that can be accessed in planner rules.
        .context(WayangRelFactories.ALL_RHEEM_REL_FACTORIES)
        // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
        //.ruleSets(RuleSets.ofList())
        // Custom cost factory to use during optimization
        .costFactory(null)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();

    RelBuilder builder = RelBuilder.create(calciteFrameworkConfig);
    RelNode root = builder.values(new String[]{"tc0"}, 1, 2, 3)
        .filter(
            builder.call(EQUALS, builder.field("tc0"), builder.literal(2))
        ).build();

    System.out.println(RelOptUtil.toString(root));

    final RelBuilderFactory builderFactory =
        RelBuilder.proto(WayangRelFactories.ALL_RHEEM_REL_FACTORIES);
//        final RelOptPlanner rel_planner = root.getCluster().getPlanner(); // VolcanoPlanner
//        for (RelOptRule r : WayangRules.ALL_RHEEM_OPT_RULES) {
//            rel_planner.addRule(r);
//        }
//        rel_planner.setRoot(root);
//
//        RelNode tmp = rel_planner.findBestExp();
//
    WayangRel.Implementor implementor = new WayangRel.Implementor();

    implementor.visitChild(0, root);
    implementor.getStatements()
        .forEach(System.out::println);
  }
}
