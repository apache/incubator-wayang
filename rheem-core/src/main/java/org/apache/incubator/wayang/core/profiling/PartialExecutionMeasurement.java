package io.rheem.rheem.core.profiling;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.optimizer.costs.TimeEstimate;
import io.rheem.rheem.core.platform.PartialExecution;

/**
 * This {@link Measurement} adapts a {@link PartialExecutionMeasurement}.
 */
@Type("partial-execution")
public class PartialExecutionMeasurement extends Measurement {

    /**
     * @see PartialExecution#getMeasuredExecutionTime()
     */
    private long executionMillis;

    /**
     * @see PartialExecution#getOverallTimeEstimate(Configuration)
     */
    private TimeEstimate estimatedExecutionMillis;


    /**
     * Serialization constructor.
     */
    private PartialExecutionMeasurement() {
    }

    /**
     * Creates a new instance.
     *
     * @param id               the ID of the new instance
     * @param partialExecution provides data for the new instance
     * @param configuration    required to calculate the estimated execution time
     */
    public PartialExecutionMeasurement(String id, PartialExecution partialExecution, Configuration configuration) {
        super(id);
        // TODO: Capture what has been executed?
        this.executionMillis = partialExecution.getMeasuredExecutionTime();
        this.estimatedExecutionMillis = partialExecution.getOverallTimeEstimate(configuration);
    }

    public long getExecutionMillis() {
        return executionMillis;
    }

    public void setExecutionMillis(long executionMillis) {
        this.executionMillis = executionMillis;
    }

    public TimeEstimate getEstimatedExecutionMillis() {
        return estimatedExecutionMillis;
    }

    public void setEstimatedExecutionMillis(TimeEstimate estimatedExecutionMillis) {
        this.estimatedExecutionMillis = estimatedExecutionMillis;
    }
}
