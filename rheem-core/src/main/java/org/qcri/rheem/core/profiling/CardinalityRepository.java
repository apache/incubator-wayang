package org.qcri.rheem.core.profiling;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

/**
 * Stores cardinalities that have been collected by the {@link CrossPlatformExecutor}. Current version uses
 * JSON as serialization format.
 */
public class CardinalityRepository {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Path to the repository file.
     */
    private final String repositoryPath;

    /**
     * Created on demand an can be closed as well.
     */
    private BufferedWriter writer;

    public CardinalityRepository(Configuration configuration) {
        this.repositoryPath = configuration.getStringProperty("rheem.core.log.cardinalities");
    }

    /**
     * Store the input and output cardinalities for all those {@link Operator}s that have a measured output
     * cardinality.
     *
     * @param executionState      contains the cardinalites of the instrumented {@link Slot}s; those cardinalities should
     *                            be already injected in the {@code rheemPlan}
     * @param optimizationContext keeps {@link CardinalityEstimate}s for all {@link Slot}s around;
     *                            it is assumed, that any measured cardinalities are already
     *                            injected in this {@link OptimizationContext} to guarantee that we capture the most
     *                            possible accurate data
     */
    public void storeAll(ExecutionState executionState, OptimizationContext optimizationContext) {
        this.logger.info("Storing cardinalities at {}.", this.repositoryPath);

//        executionState.getCardinalityMeasurements().forEach(
//                channelInstance -> {
//                    for (Slot<?> correspondingSlot : channelInstance.getChannel().getCorrespondingSlots()) {
//                        for (Slot<?> slot : OptimizationUtils.collectConnectedSlots(correspondingSlot)) {
//                            if (slot instanceof OutputSlot<?>) {
//                                OutputSlot<Object> outputSlot = ((OutputSlot<?>) slot).unchecked();
//                                final Operator operator = outputSlot.getOwner();
//                                if (!operator.isElementary() || operator.isSource()) {
//                                    continue;
//                                }
//                                final OptimizationContext.OperatorContext operatorContext = channelInstance.getProducerOperatorContext();
//                                if (operatorContext == null) {
//                                    // TODO: Handle cardinalities inside of loops.
//                                    this.logger.debug("Could not inject measured cardinality for {}: " +
//                                            "It is presumably a glue operator or inside of a loop.", operator);
//                                    continue;
//                                }
//                                this.store(outputSlot, channelInstance.getMeasuredCardinality().getAsLong(), operatorContext);
//                            }
//                        }
//                    }
//                });
        this.logger.warn("Cardinality repository currently disabled.");
    }

    /**
     * Stores the {@code cardinality} for the {@code output} together with its {@link Operator} and input
     * {@link CardinalityEstimate}s.
     */
    public void store(OutputSlot<?> output, long cardinality, OptimizationContext.OperatorContext operatorContext) {
        assert output.getOwner() == operatorContext.getOperator() :
                String.format("Owner of %s is not %s.", output, operatorContext.getOperator());
        if (!operatorContext.getOutputCardinality(output.getIndex()).isExactly(cardinality)) {
            this.logger.error("Expected a measured cardinality of {} for {}; found {}.",
                    cardinality, output, operatorContext.getOutputCardinality(output.getIndex()));
        }

        this.write(operatorContext, output, cardinality);
    }

    private void write(OptimizationContext.OperatorContext operatorContext,
                       OutputSlot<?> output,
                       long outputCardinality) {

        JSONArray jsonInputCardinalities = new JSONArray();
        final Operator operator = operatorContext.getOperator();
        for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
            final InputSlot<?> input = operator.getInput(inputIndex);
            final CardinalityEstimate inputEstimate = operatorContext.getInputCardinality(inputIndex);

            JSONObject jsonInputCardinality = new JSONObject();
            jsonInputCardinality.put("name", input.getName());
            jsonInputCardinality.put("index", input.getIndex());
            jsonInputCardinality.put("isBroadcast", input.isBroadcast());
            jsonInputCardinality.put("lowerBound", inputEstimate.getLowerEstimate());
            jsonInputCardinality.put("upperBound", inputEstimate.getUpperEstimate());
            jsonInputCardinality.put("confidence", inputEstimate.getCorrectnessProbability());
            jsonInputCardinalities.put(jsonInputCardinality);
        }

        JSONObject jsonOperator = new JSONObject();
        jsonOperator.put("class", operator.getClass().getCanonicalName());
        // TODO: UDFs? How can we reference them?

        JSONObject jsonOutput = new JSONObject();
        jsonOutput.put("name", output.getName());
        jsonOutput.put("index", output.getIndex());
        jsonOutput.put("cardinality", outputCardinality);

        JSONObject jsonMeasurement = new JSONObject();
        jsonMeasurement.put("inputs", jsonInputCardinalities);
        jsonMeasurement.put("operator", jsonOperator);
        jsonMeasurement.put("output", jsonOutput);

        this.write(jsonMeasurement);
    }

    /**
     * Writes the measuremnt to the {@link #repositoryPath}.
     */
    private void write(JSONObject jsonMeasurement) {
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
                throw new RheemException("Could not initialize cardinality repository.");
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
