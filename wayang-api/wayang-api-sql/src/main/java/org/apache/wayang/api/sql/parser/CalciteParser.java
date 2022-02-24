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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.wayang.api.sql.relation.node.WayangRelFactories;
import org.apache.wayang.api.sql.rule.WayangRules;


public class CalciteParser {
  public Planner planner;

  public CalciteParser(SchemaPlus schema) {
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

    this.planner = Frameworks.getPlanner(calciteFrameworkConfig);

  }

  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }


    System.out.println(planner.validateAndGetType(sqlNode));

    return planner.rel(sqlNode).project();
  }

  public RelNode toPhisicalPlan(RelNode logicalPlan) throws RelConversionException {
    final RelBuilderFactory builderFactory =
        RelBuilder.proto(WayangRelFactories.ALL_RHEEM_REL_FACTORIES);
    final RelOptPlanner planner = logicalPlan.getCluster().getPlanner(); // VolcanoPlanner
    List<RelOptRule> rm = planner.getRules();
    for (RelOptRule r : rm){
      planner.removeRule(r);
    }

    for (RelOptRule r : WayangRules.ALL_WAYANG_OPT_RULES) {
      planner.addRule(r);
    }
    planner.setRoot(logicalPlan);
    System.out.println(planner.getContext().toString());
    System.out.println(planner.getRules());
    //return planner.findBestExp();
    return planner.getRoot();
  }

  public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException, SqlParseException {
    // Simple connection implementation for loading schema from sales.json
    CalciteConnection connection = new SimpleCalciteConnection();
    String salesSchema = Resources.toString(CalciteParser.class.getResource("/WayangModel.json"), Charset.defaultCharset());
    // ModelHandler reads the sales schema and load the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + salesSchema);

    // Create the query planner with sales schema. conneciton.getSchema returns default schema name specified in sales.json
    CalciteParser queryPlanner = new CalciteParser(connection.getRootSchema().getSubSchema(connection.getSchema()));
    RelNode logicalPlan = queryPlanner
        .getLogicalPlan(
            "select EXPR$0 from (values (1), (2)) where EXPR$0 > 1"
            //"select t.tc0 from t where t.tc0 = 'hello'"
        );



    System.out.println(RelOptUtil.toString(logicalPlan));

    RelNode tmp = queryPlanner.toPhisicalPlan(logicalPlan);

    System.out.println(RelOptUtil.toString(tmp));
  }
}
