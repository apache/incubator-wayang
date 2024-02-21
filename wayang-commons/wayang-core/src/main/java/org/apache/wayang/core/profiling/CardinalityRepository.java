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

package org.apache.wayang.core.profiling;

import org.apache.commons.io.IOUtils;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.Slot;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.optimizer.OptimizationUtils;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.platform.AtomicExecutionGroup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import org.apache.wayang.core.util.json.WayangJsonArray;
import org.apache.wayang.core.util.json.WayangJsonObj;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stores cardinalities that have been collected by the {@link CrossPlatformExecutor}. Current version uses
 * JSON as serialization format.
 */
public class CardinalityRepository {

    private final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Path to the repository file.
     */
    private final String repositoryPath;

    /**
     * Created on demand an can be closed as well.
     */
    private BufferedWriter writer;

    public CardinalityRepository(Configuration configuration) {
        this.repositoryPath = configuration.getStringProperty("wayang.core.log.cardinalities");
    }

    /**
     * Store the input and output cardinalities for all those {@link Operator}s that have a measured output
     * cardinality.
     *
     * @param executionState      contains the cardinalites of the instrumented {@link Slot}s; those cardinalities should
     *                            be already injected in the {@code wayangPlan}
     * @param optimizationContext keeps {@link CardinalityEstimate}s for all {@link Slot}s around;
     *                            it is assumed, that any measured cardinalities are already
     *                            injected in this {@link OptimizationContext} to guarantee that we capture the most
     *                            possible accurate data
     */
    public void storeAll(ExecutionState executionState, OptimizationContext optimizationContext) {
        this.logger.info("Storing cardinalities at {}.", this.repositoryPath);

        executionState.getCardinalityMeasurements().forEach(
                channelInstance -> {
                    Set<Slot<?>> distinctSlots = channelInstance
                        .getChannel()
                        .getCorrespondingSlots()
                        .stream()
                        .map(slot -> OptimizationUtils.collectConnectedSlots(slot))
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
                    for (Slot<?> slot : distinctSlots) {
                        if (slot instanceof OutputSlot<?>) {
                            OutputSlot<Object> outputSlot = ((OutputSlot<?>) slot).unchecked();
                            final Operator operator = outputSlot.getOwner();
                            if (!operator.isElementary() || operator.isSource()) {
                                continue;
                            }
                            final OptimizationContext.OperatorContext operatorContext = channelInstance.getProducerOperatorContext();
                            if (operatorContext == null) {
                                // TODO: Handle cardinalities inside of loops.
                                this.logger.debug("Could not inject measured cardinality for {}: " +
                                        "It is presumably a glue operator or inside of a loop.", operator);
                                continue;
                            }
                            this.store(outputSlot, channelInstance.getMeasuredCardinality().getAsLong(), operatorContext, operator);
                        }
                    }
                });
        //this.logger.warn("Cardinality repository currently disabled.");
    }

    /**
     * Stores the {@code cardinality} for the {@code output} together with its {@link Operator} and input
     * {@link CardinalityEstimate}s.
     */
    public void store(
            OutputSlot<?> output,
            long cardinality,
            OptimizationContext.OperatorContext operatorContext,
            Operator operator) {
        assert output.getOwner() == operator :
                String.format("Owner of %s is not %s.", output, operatorContext.getOperator());
        if (!operatorContext.getOutputCardinality(output.getIndex()).isExactly(cardinality)) {
            this.logger.error("Expected a measured cardinality of {} for {}; found {}.",
                    cardinality, output, operatorContext.getOutputCardinality(output.getIndex()));
        }

        this.write(operatorContext, output, cardinality, operator);
    }

    private void write(OptimizationContext.OperatorContext operatorContext,
                       OutputSlot<?> output,
                       long outputCardinality,
                       Operator operator) {

        WayangJsonArray jsonInputCardinalities = new WayangJsonArray();
        for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
            final InputSlot<?> input = operator.getInput(inputIndex);
            final CardinalityEstimate inputEstimate = operatorContext.getInputCardinality(inputIndex);

            WayangJsonObj jsonInputCardinality = new WayangJsonObj();
            jsonInputCardinality.put("name", input.getName());
            jsonInputCardinality.put("index", input.getIndex());
            jsonInputCardinality.put("isBroadcast", (Boolean) input.isBroadcast());
            jsonInputCardinality.put("lowerBound", inputEstimate.getLowerEstimate());
            jsonInputCardinality.put("upperBound", inputEstimate.getUpperEstimate());
            jsonInputCardinality.put("confidence", inputEstimate.getCorrectnessProbability());
            jsonInputCardinalities.put(jsonInputCardinality);
        }

        WayangJsonObj jsonOperator = new WayangJsonObj();
        jsonOperator.put("class", operator.getClass().getCanonicalName());
        // TODO: UDFs? How can we reference them?

        WayangJsonObj jsonOutput = new WayangJsonObj();
        jsonOutput.put("name", output.getName());
        jsonOutput.put("index", output.getIndex());
        jsonOutput.put("cardinality", outputCardinality);

        WayangJsonObj jsonMeasurement = new WayangJsonObj();
        jsonMeasurement.put("inputs", jsonInputCardinalities);
        jsonMeasurement.put("operator", jsonOperator);
        jsonMeasurement.put("output", jsonOutput);

        this.write(jsonMeasurement);
    }

    /**
     * Writes the measuremnt to the {@link #repositoryPath}.
     */
    private void write(WayangJsonObj jsonMeasurement) {
        try {
            jsonMeasurement.write(this.getWriter());
            writer.write('\n');
        } catch (IOException e) {
            IOUtils.closeQuietly(this.writer);
            throw new RuntimeException("Could not open cardinality repository file for writing.", e);
        }
    }

    /**
     * Initializes the {@link #writer} if it does not exist currently.
     *
     * @return the {@link #writer}
     */
    private BufferedWriter getWriter() throws FileNotFoundException, UnsupportedEncodingException {
        if (this.writer == null) {
            File file = new File(this.repositoryPath);
            final File parentFile = file.getParentFile();
            if (!parentFile.exists() && !file.getParentFile().mkdirs()) {
                throw new WayangException("Could not initialize cardinality repository.");
            }
            this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));
        }
        return this.writer;
    }

    /**
     * Allows this instance to free its system resources, as they might not be needed in the closer future.
     */
    public void sleep() {
        IOUtils.closeQuietly(this.writer);
        this.writer = null;
    }
}
