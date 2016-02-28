package org.qcri.rheem.core.profiling;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

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

    public CardinalityRepository(String repositoryPath) {
        this.repositoryPath = repositoryPath;
        this.logger.info("Storing cardinalities at {}.", repositoryPath);
    }

    /**
     * Store the input and output cardinalities for all those {@link Operator}s that have a measured output
     * cardinality.
     *
     * @param executionProfile contains the cardinalites of the instrumented {@link Slot}s; those cardinalities should
     *                         be already injected in the {@code rheemPlan}
     * @param rheemPlan        that has been executed; it is assumed, that any measured cardinalities are already
     *                         injected in this {@link RheemPlan} to guarantee that we capture the most possible
     *                         accurate data
     */
    public void storeAll(ExecutionProfile executionProfile, RheemPlan rheemPlan) {
        final Map<Channel, Long> cardinalities = executionProfile.getCardinalities();
        for (Map.Entry<Channel, Long> entry : cardinalities.entrySet()) {
            final Channel channel = entry.getKey();
            final long cardinality = entry.getValue();
            for (Slot<?> slot : channel.getCorrespondingSlots()) {
                if (slot instanceof OutputSlot<?>) {
                    this.store((OutputSlot<?>) slot, cardinality, rheemPlan);
                }
            }
        }
    }

    /**
     * Stores the {@code cardinality} for the {@code output} together with its {@link Operator} and input
     * {@link CardinalityEstimate}s.
     */
    public void store(OutputSlot<?> output, long cardinality, RheemPlan rheemPlan) {
        assert output.getCardinalityEstimate().isExactly(cardinality)
                : String.format("Expected a measured cardinality for %s; found %s.",
                output, output.getCardinalityEstimate());

        final Operator owner = output.getOwner();
        final InputSlot<?>[] allInputs = owner.getAllInputs();
        this.write(allInputs, owner, output, cardinality);
    }

    private void write(InputSlot<?>[] inputs, Operator operator, OutputSlot<?> output, long outputCardinality) {
        JSONArray jsonInputCardinalities = new JSONArray();
        for (InputSlot<?> input : inputs) {
            JSONObject jsonInputCardinality = new JSONObject();
            jsonInputCardinality.put("name", input.getName());
            jsonInputCardinality.put("index", input.getIndex());
            jsonInputCardinality.put("isBroadcast", input.isBroadcast());
            jsonInputCardinality.put("lowerBound", input.getCardinalityEstimate().getLowerEstimate());
            jsonInputCardinality.put("upperBound", input.getCardinalityEstimate().getUpperEstimate());
            jsonInputCardinality.put("confidence", input.getCardinalityEstimate().getCorrectnessProbability());
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
