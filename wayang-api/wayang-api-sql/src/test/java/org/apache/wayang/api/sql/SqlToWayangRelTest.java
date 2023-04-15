/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.wayang.api.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.WayangSchema;
import org.apache.wayang.api.sql.calcite.schema.WayangSchemaBuilder;
import org.apache.wayang.api.sql.calcite.schema.WayangTable;
import org.apache.wayang.api.sql.calcite.schema.WayangTableBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;


public class SqlToWayangRelTest {

    @Test
    public void test_simple_sql() throws Exception {
        WayangTable customer = WayangTableBuilder.build("customer")
                .addField("id", SqlTypeName.INTEGER)
                .addField("name", SqlTypeName.VARCHAR)
                .addField("age", SqlTypeName.INTEGER)
                .withRowCount(100)
                .build();

        WayangTable orders = WayangTableBuilder.build("orders")
                .addField("id", SqlTypeName.INTEGER)
                .addField("cid", SqlTypeName.INTEGER)
                .addField("price", SqlTypeName.DECIMAL)
                .addField("quantity", SqlTypeName.INTEGER)
                .withRowCount(100)
                .build();

        WayangSchema wayangSchema = WayangSchemaBuilder.build("exSchema")
                .addTable(customer)
                .addTable(orders)
                .build();

        Optimizer optimizer = Optimizer.create(wayangSchema);

//        String sql = "select c.name, c.age from customer c where (c.age < 40 or c.age > 60) and \'alex\' = c.name";
//        String sql = "select c.age from customer c";
        String sql = "select c.name, c.age, o.price from customer c join orders o on c.id = o.cid where c.age > 40 " +
                "and o" +
                ".price < 100";


        SqlNode sqlNode = optimizer.parseSql(sql);
        SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        RelNode relNode = optimizer.convert(validatedSqlNode);

        print("After parsing", relNode);


        RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_JOIN_RULE
        );

        RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules
        );

        print("After rel to wayang conversion", wayangRel);


        //WayangPlan plan = optimizer.convert(wayangRel);

        //print("After Translating to WayangPlan", plan);



    }


    private void print(String header, WayangPlan plan) {
        StringWriter sw = new StringWriter();
        sw.append(header).append(":").append("\n");

        final Collection<Operator> operators = PlanTraversal.upstream().traverse(plan.getSinks()).getTraversedNodes();
        operators.forEach(o -> sw.append(o.toString()));

        System.out.println(sw.toString());
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());
    }

}
