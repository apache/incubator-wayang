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

import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.Jobs;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.ml.util.CardinalitySampler;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.util.Set;

public class Training {

    /*
     * args format:
     * 1: platforms, comma sep. (string)
     * 2: encode to file path (string)
     * 3: overwrite cardinalities (boolean)
     **/
    public static void main(String[] args) {
        Set<Class<? extends GeneratableJob>> jobs = Jobs.getJobs();
        for (Class<? extends GeneratableJob> job : jobs) {
            System.out.println(job);
            try {
                Constructor<?> cnstr = job.getDeclaredConstructors()[0];
                GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
                String[] jobArgs = {args[0]};
                FileWriter fw = new FileWriter(args[1], false);
                BufferedWriter writer = new BufferedWriter(fw);


                boolean overwriteCardinalities = true;

                if (args.length == 3) {
                    overwriteCardinalities = Boolean.valueOf(args[2]);
                }
                /*
                 * TODO:
                 *  - Get DataQuanta's WayangPlan :done:
                 *  - Encode WayangPlan and print/store :done:
                 *  -- We need to run it once before, so that we can get card estimates :done:
                 *  - Call .buildInitialExecutionPlan for the WayangPlan :done:
                 *  - Encode the ExecutionPlan and print/store :done:
                 *  - Make cardinalities configurable
                 */

                DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
                PlanBuilder builder = quanta.getPlanBuilder();
                WayangContext context = builder.getWayangContext();
                Configuration config = context.getConfiguration();
                WayangPlan plan = builder.build();

                int hashCode = new HashCodeBuilder(17, 37).append(plan).toHashCode();
                String path = "/var/www/html/data/" + hashCode + "-cardinalities.json";

                if (overwriteCardinalities) {
                    CardinalitySampler.configureWriteToFile(config, path);
                    context.execute(plan, "");

                    quanta = createdJob.buildPlan(jobArgs);
                    builder = quanta.getPlanBuilder();
                    context = builder.getWayangContext();
                    plan = builder.build();
                }

                CardinalitySampler.readFromFile(path);
                TreeNode wayangNode = TreeEncoder.encode(plan, context);
                TreeNode execNode = TreeEncoder.encode(context.buildInitialExecutionPlan("", plan, "")).withIdsFrom(wayangNode);

                System.out.println(wayangNode);
                System.out.println(execNode);

                writer.write(String.format("%s:%s", wayangNode.toString(), execNode.toString()));
                writer.newLine();
                writer.flush();
                /*
                 * TODO: (LATER)
                 *  - Randomize platform usage as args
                 */
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
