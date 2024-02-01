/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.examples;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;
import org.apache.wayang.ml4all.abstraction.plan.ML4allPlan;
import org.apache.wayang.ml4all.algorithms.kmeans.*;
import org.apache.wayang.spark.Spark;

import java.net.MalformedURLException;
import java.util.Arrays;

public class RunKMeans {

    public static void main(String[] args) throws MalformedURLException {

        int numberOfCentroids = 3;
        int dimension = 68;
        double accuracy = 0;
        int maxIterations = 10;

        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <input file URL> <numberOfCentroids> <dimension> <accuracy> <maxIterations>");
            System.exit(1);
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
        String file = args[1];
        numberOfCentroids = Integer.parseInt(args[2]);
        dimension = Integer.parseInt(args[3]);
        accuracy = Double.parseDouble(args[4]);
        maxIterations = Integer.parseInt(args[5]);

        long start_time = System.currentTimeMillis();

        ML4allPlan plan = new ML4allPlan();

        //logical operators of template
        plan.setTransformOp(new TransformCSV());
        plan.setLocalStage(new KMeansStageWithZeros(numberOfCentroids, dimension));
        plan.setComputeOp(new KMeansCompute());
        plan.setUpdateOp(new KMeansUpdate());
        plan.setLoopOp(new KMeansConvergeOrMaxIterationsLoop(accuracy, maxIterations));

        ML4allModel ml4allModel = plan.execute(file, wayangContext);
        System.out.println("Centers:" + Arrays.deepToString((double [][])ml4allModel.getByKey("centers")));

        System.out.println("Total time: " + (System.currentTimeMillis() - start_time));
    }

}
