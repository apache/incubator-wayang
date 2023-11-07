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

import org.apache.wayang.ml4all.abstraction.plan.ML4allContext;
import org.apache.wayang.ml4all.abstraction.plan.ML4allPlan;
import org.apache.wayang.ml4all.abstraction.plan.Platforms;
import org.apache.wayang.ml4all.algorithms.kmeans.*;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Arrays;

import static org.apache.wayang.ml4all.abstraction.plan.Platforms.*;

public class RunKMeans {

    public static void main(String[] args) throws MalformedURLException {

        int numberOfCentroids = 3;
//        String file = "fsrc/main/resources/input/kmeans_data.txt";
//        int dimension = 3;
        String relativePath = "wayang-ml4all/src/main/resources/input/USCensus1990-sample.input";
        String file = new File(relativePath).getAbsoluteFile().toURI().toURL().toString();
        String propertiesFile = new File("wayang-ml4all/src/main/resources/wayang.properties").getAbsoluteFile().toURI().toURL().toString();
        int dimension = 68;
        double accuracy = 0;
        int maxIterations = 10;
        Platforms platform = SPARK;

        //Usage:  <data_file> <#points> <#dimension> <#centroids> <accuracy>
        if (args.length > 0) {
            file = args[0];
            dimension = Integer.parseInt(args[1]);
            numberOfCentroids = Integer.parseInt(args[2]);
            accuracy = Double.parseDouble(args[3]);
            maxIterations = Integer.parseInt(args[4]);
            String platformIn = args[5];
        }
        else {
            System.out.println("Loading default values");
        }

        long start_time = System.currentTimeMillis();

        ML4allPlan plan = new ML4allPlan();

        //logical operators of template
        plan.setTransformOp(new TransformCSV());
        plan.setLocalStage(new KMeansStageWithZeros(numberOfCentroids, dimension));
        plan.setComputeOp(new KMeansCompute());
        plan.setUpdateOp(new KMeansUpdate());
        plan.setLoopOp(new KMeansConvergeOrMaxIterationsLoop(accuracy, maxIterations));

        ML4allContext context = plan.execute(file, platform, propertiesFile);
        System.out.println("Centers:" + Arrays.deepToString((double [][])context.getByKey("centers")));

        System.out.println("Total time: " + (System.currentTimeMillis() - start_time));
    }

}
