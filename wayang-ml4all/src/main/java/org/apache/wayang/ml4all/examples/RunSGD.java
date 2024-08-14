/*
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
import org.apache.wayang.ml4all.algorithms.sgd.*;
import org.apache.wayang.spark.Spark;

import java.net.MalformedURLException;
import java.util.Arrays;

public class RunSGD {

    // Default parameters.
    static String fileURL;
    static int datasetSize  = 100827;
    static int features = 123;

    //these are for SGD/mini run to convergence
    static double accuracy = 0.001;
    static int max_iterations = 1000;


    public static void main (String... args) throws MalformedURLException {

        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <input file URL> <dataset size> <#features> <max maxIterations> <accuracy>");
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

        //Usage: <data_file> <#features> <sparse> <binary>
        if (args.length > 0) {
            fileURL = args[1];
            datasetSize = Integer.parseInt(args[2]);
            features = Integer.parseInt(args[3]);
            max_iterations = Integer.parseInt(args[4]);
            accuracy = Double.parseDouble(args[5]);
        }

        System.out.println("max #maxIterations:" + max_iterations);
        System.out.println("accuracy:" + accuracy);

        long start_time = System.currentTimeMillis();
        ML4allPlan plan = new ML4allPlan();
        plan.setDatasetsize(datasetSize);

        //logical operators of template
        plan.setTransformOp(new LibSVMTransform(features));
        plan.setLocalStage(new SGDStageWithZeros(features));
        plan.setSampleOp(new SGDSample());
        plan.setComputeOp(new ComputeLogisticGradient());
        plan.setUpdateLocalOp(new WeightsUpdate());
        plan.setLoopOp(new SGDLoop(accuracy, max_iterations));

        ML4allModel ml4allModel = plan.execute(fileURL, wayangContext);
        System.out.println("Training finished in " + (System.currentTimeMillis() - start_time));
        System.out.println(ml4allModel);
        System.out.println("Weights:" + Arrays.toString((double [])ml4allModel.getByKey("weights")));
    }

}
