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

package org.apache.wayang.apps.tpch;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.apps.tpch.queries.Query1Wayang;
import org.apache.wayang.apps.tpch.queries.Query3;
import org.apache.wayang.apps.tpch.queries.Query5;
import org.apache.wayang.apps.tpch.queries.Query6;
import org.apache.wayang.apps.tpch.queries.Query10;
import org.apache.wayang.apps.tpch.queries.Query12;
import org.apache.wayang.apps.tpch.queries.Query14;
import org.apache.wayang.apps.tpch.queries.Query19;

/**
 * Main class for the TPC-H app based on Apache Wayang (incubating).
 */
public class Main {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <query number> <query parameters>*");
            System.exit(1);
        }

        WayangPlan wayangPlan;
        switch (Integer.parseInt(args[1])) {
            case 1:
                wayangPlan = Query1Wayang.createPlan(args[2], Integer.parseInt(args[3]));
                break;
            case 3:
                wayangPlan = Query3.createPlan(args[2], args[3], args[4], Integer.parseInt(args[5]));
                break;
            case 5:
                wayangPlan = Query5.createPlan(args[2], args[3], args[4], args[5], args[6], args[7], Integer.parseInt(args[8]));
                break;
            case 6:
                wayangPlan = Query6.createPlan(args[2], Integer.parseInt(args[3]));
                break;
            case 10:
                wayangPlan = Query10.createPlan(args[2], args[3], args[4], args[5], Integer.parseInt(args[6]));
                break;
            case 12:
                wayangPlan = Query12.createPlan(args[2], args[3], Integer.parseInt(args[4]));
                break;
            case 14:
                wayangPlan = Query14.createPlan(args[2], args[3], Integer.parseInt(args[4]));
                break;
            case 19:
                wayangPlan = Query19.createPlan(args[2], args[3], Integer.parseInt(args[4]));
                break;
            default:
                System.err.println("Unsupported query number.");
                System.exit(2);
                return;
        }

        WayangContext wayangContext = new WayangContext();
        for (String platform : args[0].split(",")) {
            switch (platform) {
                case "java":
                    wayangContext.register(Java.basicPlugin());
                    break;
                case "spark":
                    wayangContext.register(Spark.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
                    return;
            }
        }

        wayangContext.execute(wayangPlan, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));
    }
}
