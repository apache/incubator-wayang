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

package org.apache.wayang.core.api;

import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.core.monitor.Monitor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.profiling.CardinalityRepository;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * This is the entry point for users to work with Wayang.
 */
public class WayangContext {

    @SuppressWarnings("unused")
    private final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Stores input/output cardinalities to provide better {@link CardinalityEstimator}s over time.
     */
    private CardinalityRepository cardinalityRepository;

    private final Configuration configuration;

    public WayangContext() {
        this(new Configuration());
    }

    public WayangContext(Configuration configuration) {
        this.configuration = configuration.fork(String.format("WayangContext(%s)", configuration.getName()));
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @return this instance
     */
    public WayangContext with(Plugin plugin) {
        this.register(plugin);
        return this;
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @return this instance
     */
    public WayangContext withPlugin(Plugin plugin) {
        this.register(plugin);
        return this;
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @see #with(Plugin)
     */
    public void register(Plugin plugin) {
        plugin.configure(this.getConfiguration());
    }

    /**
     * Execute a plan.
     *
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(WayangPlan wayangPlan, String... udfJars) {
        this.execute(null, wayangPlan, udfJars);
    }

    /**
     * Execute a plan.
     *
     * @param jobName   name of the {@link Job} or {@code null}
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, WayangPlan wayangPlan, String... udfJars) {
        this.execute(jobName, null, wayangPlan, udfJars);
    }

    /**
     * Execute a plan.
     *
     * @param jobName   name of the {@link Job} or {@code null}
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, Monitor monitor, WayangPlan wayangPlan, String... udfJars) {
        this.createJob(jobName, monitor, wayangPlan, udfJars).execute();
    }

    /**
     * Execute a plan.
     *
     * @param jobName    name of the {@link Job} or {@code null}
     * @param wayangPlan  the plan to execute
     * @param experiment {@link Experiment} for that profiling entries will be created
     * @param udfJars    JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, WayangPlan wayangPlan, Experiment experiment, String... udfJars) {
        this.createJob(jobName, wayangPlan, experiment, udfJars).execute();
    }

    public void explain(WayangPlan wayangPlan, String... udfJars) {
        Job job = this.createJob(null, null, wayangPlan, udfJars);
        ExecutionPlan executionPlan = job.buildInitialExecutionPlan();

        ExplainUtils.parsePlan(wayangPlan, false);
        System.out.println();
        ExplainUtils.parsePlan(executionPlan, false);
    }

    public void explain(WayangPlan wayangPlan, boolean toJson, String... udfJars) {
        Job job = this.createJob(null, null, wayangPlan, udfJars);
        ExecutionPlan executionPlan = job.buildInitialExecutionPlan();

        ExplainUtils.write(
            ExplainUtils.parsePlan(wayangPlan, true),
            this.configuration.getStringProperty("wayang.core.explain.logical.file")
        );
        ExplainUtils.write(
            ExplainUtils.parsePlan(executionPlan, true),
            this.configuration.getStringProperty("wayang.core.explain.execution.file")
        );
    }


    /**
     * Build an execution plan.
     *
     * @param wayangPlan the plan to translate
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public ExecutionPlan buildInitialExecutionPlan(String jobName, WayangPlan wayangPlan, String... udfJars) {
        return this.createJob(jobName, null, wayangPlan, udfJars).buildInitialExecutionPlan();
    }

    /**
     * Create a new {@link Job} that should execute the given {@link WayangPlan} eventually.
     *
     * @param experiment {@link Experiment} for that profiling entries will be created
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, WayangPlan wayangPlan, Experiment experiment, String... udfJars) {
        return new Job(this, jobName, null, wayangPlan, experiment, udfJars);
    }

    /**
     * Create a new {@link Job} that should execute the given {@link WayangPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, WayangPlan wayangPlan, String... udfJars) {
        return this.createJob(jobName, null, wayangPlan, udfJars);
    }

    /**
     * Create a new {@link Job} that should execute the given {@link WayangPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, Monitor monitor, WayangPlan wayangPlan, String... udfJars) {
        return new Job(this, jobName, monitor, wayangPlan, new Experiment("unknown", new Subject("unknown", "unknown")), udfJars);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public CardinalityRepository getCardinalityRepository() {
        if (this.cardinalityRepository == null) {
            this.cardinalityRepository = new CardinalityRepository(this.configuration);
        }
        return this.cardinalityRepository;
    }

    public WayangContext setLogLevel(Level level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
        ctx.updateLoggers();

        return this;
    }
}
