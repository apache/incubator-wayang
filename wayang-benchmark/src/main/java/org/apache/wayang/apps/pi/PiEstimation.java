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
package org.apache.wayang.apps.pi;



import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PiEstimation {
    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                System.exit(1);
            }



            int slices = (args.length > 1) ? Integer.parseInt(args[1]) : 2;
            int n = 100000 * slices; // Total points

            var wayangContext = new WayangContext(new Configuration());
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

            var planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("Wayang Pi Estimation");

            // Create dataset of numbers from 0 to n
            List<Integer> numbers = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                numbers.add(i);
            }

            // Wayang data pipeline
            long count = planBuilder
                    .loadCollection(numbers)
                    .map(i -> {
                        double x = ThreadLocalRandom.current().nextDouble(-1, 1);
                        double y = ThreadLocalRandom.current().nextDouble(-1, 1);
                        return (x * x + y * y <= 1) ? 1 : 0;
                    })
                    .reduce(Integer::sum)
                    .collect()
                    .iterator().next();

            // Estimate Pi
            double pi = 4.0 * count / n;
            System.out.println("Pi is roughly: " + pi);

        } catch (NumberFormatException e) {
            System.err.println("Invalid number format for slices. Please provide an integer.");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.println("Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}