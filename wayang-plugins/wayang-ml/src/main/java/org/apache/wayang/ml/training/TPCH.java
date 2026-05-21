/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wayang.ml.training;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;

import java.util.HashMap;
import java.util.List;
import scala.collection.Seq;
import scala.collection.JavaConversions;

public class TPCH {
    /**
     * 0: platforms
     * 1: TPCH data set directory path
     * 2: query number
     */
    public static void main(String[] args) {
        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
        Configuration config = new Configuration();
        final WayangContext wayangContext = new WayangContext(config);
        plugins.stream().forEach(plug -> wayangContext.register(plug));

        HashMap<String, WayangPlan> plans = createPlans(args[1]);
        WayangPlan plan = plans.get("query" + args[2]);

        System.out.println("Executing query " + args[2]);
        wayangContext.execute(plan, "");
        System.out.println("Finished execution");
    }

    public static HashMap<String, WayangPlan> createPlans(String dataDir) {
        String url = "file://" + dataDir;

        String lineItemUrl = url + "/lineitem.tbl";
        String customerUrl = url + "/customer.tbl";
        String orderUrl = url + "/orders.tbl";
        String supplierUrl = url + "/supplier.tbl";
        String regionUrl = url + "/region.tbl";
        String nationUrl = url + "/nation.tbl";
        String partUrl = url + "/part.tbl";
        String partSuppUrl = url + "/partsupp.tbl";

        HashMap<String, WayangPlan> tpchPlans = new HashMap<>();
        tpchPlans.put("query1", org.apache.wayang.apps.tpch.queries.Query1Wayang.createPlan(lineItemUrl, 1));
        tpchPlans.put("query3", org.apache.wayang.apps.tpch.queries.Query3.createPlan(customerUrl, orderUrl, lineItemUrl, 1));
        tpchPlans.put("query5", org.apache.wayang.apps.tpch.queries.Query5.createPlan(customerUrl, orderUrl, lineItemUrl, supplierUrl, nationUrl, regionUrl, 1));
        tpchPlans.put("query6", org.apache.wayang.apps.tpch.queries.Query6.createPlan(lineItemUrl, 1));
        tpchPlans.put("query10", org.apache.wayang.apps.tpch.queries.Query10.createPlan(customerUrl, orderUrl, lineItemUrl, nationUrl, 1));
        tpchPlans.put("query12", org.apache.wayang.apps.tpch.queries.Query12.createPlan(orderUrl, lineItemUrl, 1));
        tpchPlans.put("query14", org.apache.wayang.apps.tpch.queries.Query14.createPlan(lineItemUrl, partUrl, 1));
        tpchPlans.put("query19", org.apache.wayang.apps.tpch.queries.Query19.createPlan(lineItemUrl, partUrl, 1));

        return tpchPlans;
      }
}

