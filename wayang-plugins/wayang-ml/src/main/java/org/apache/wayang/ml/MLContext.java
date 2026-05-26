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

package org.apache.wayang.ml;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OrtMLModel;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.Logging;

/**
 * This is the entry point for users to work with Wayang ML.
 */
public class MLContext extends WayangContext {
    public MLContext() {
        super();
    }

    public MLContext(final Configuration configuration) {
        super(configuration);
    }

    /**
     * Execute a plan.
     *
     * @param wayangPlan the plan to execute
     * @param udfJars    JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    @Override
    public void execute(final WayangPlan wayangPlan, final String... udfJars) {
        this.setLogLevel(Level.ERROR);
        final Job wayangJob = this.createJob("", wayangPlan, udfJars);

        final Configuration config = this.getConfiguration();
        final Configuration jobConfig = wayangJob.getConfiguration();

        wayangJob.execute();

        if (config.getBooleanProperty("wayang.ml.experience.enabled")) {
            final Optional<String> originalOption = config.getOptionalStringProperty("wayang.ml.experience.original");
            final String original = originalOption.orElse(TreeEncoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false).toString());

            final Optional<String> choicesOption = config
                    .getOptionalStringProperty("wayang.ml.experience.with-platforms");
            final String withChoices = choicesOption
                    .orElse(jobConfig.getStringProperty("wayang.ml.experience.with-platforms"));

            final long execTime = jobConfig.getLongProperty("wayang.ml.experience.exec-time");

            this.logExperience(original, withChoices, execTime);
        }
    }

    public void executeVAE(final WayangPlan wayangPlan, final String... udfJars) {
        this.setLogLevel(Level.ERROR);
        try {
            final Job job = this.createJob("", wayangPlan, udfJars);

            // Log Encoding time
            final Instant start = Instant.now();
            final TreeNode wayangNode = TreeEncoder.encode(wayangPlan, job.getOptimizationContext(), false);
            final Instant end = Instant.now();
            final long execTime = Duration.between(start, end).toMillis();
            Logging.writeToFile(String.format("Encoding: %d", execTime),
                    this.getConfiguration().getStringProperty("wayang.ml.optimizations.file"));
            final OrtMLModel model = OrtMLModel.getInstance(job.getConfiguration());

            // Log inference time
            final Instant inferenceStart = Instant.now();
            final Tuple<WayangPlan, TreeNode> resultTuple = model.runVAE(wayangPlan, wayangNode);
            final Instant inferenceEnd = Instant.now();
            final long inferenceTime = Duration.between(inferenceStart, inferenceEnd).toMillis();
            Logging.writeToFile(String.format("Inference: %d", inferenceTime),
                    this.getConfiguration().getStringProperty("wayang.ml.optimizations.file"));

            final WayangPlan platformPlan = resultTuple.field0;

            this.getConfiguration().setProperty("wayang.ml.experience.original", wayangNode.toStringEncoding());
            this.getConfiguration().setProperty("wayang.ml.experience.with-platforms", resultTuple.field1.toString());

            this.execute(platformPlan, udfJars);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new WayangException("Executing WayangPlan with VAE model failed");
        }
    }

    public ExecutionPlan buildWithVAE(final WayangPlan wayangPlan, final String... udfJars) {
        try {
            final Job job = this.createJob("", wayangPlan, udfJars);
            final TreeNode wayangNode = TreeEncoder.encode(wayangPlan, job.getOptimizationContext(), false);
            final OrtMLModel model = OrtMLModel.getInstance(job.getConfiguration());
            final Tuple<WayangPlan, TreeNode> resultTuple = model.runVAE(wayangPlan, wayangNode);
            final WayangPlan platformPlan = resultTuple.field0;

            return this.buildInitialExecutionPlan("", platformPlan, udfJars);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new WayangException("Executing WayangPlan with VAE model failed");
        }
    }

    private void logExperience(final String original, final String withChoices, final long execTime) {
        if (!this.getConfiguration().getBooleanProperty("wayang.ml.experience.enabled")) {
            return;
        }

        final String content = String.format("%s:%s:%d", original, withChoices, execTime);
        Logging.writeToFile(content, this.getConfiguration().getStringProperty("wayang.ml.experience.file"));
    }
}
