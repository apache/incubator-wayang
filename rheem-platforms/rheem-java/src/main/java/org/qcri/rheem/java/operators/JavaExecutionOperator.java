package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.ResourceFunction;
import org.qcri.rheem.core.optimizer.costs.ResourceUsageEstimate;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.plugin.Activator;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Execution operator for the Java platform.
 */
public interface JavaExecutionOperator extends ExecutionOperator {

    @Override
    default Platform getPlatform() {
        return Activator.PLATFORM;
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputStreams {@link Stream}s that satisfy the inputs of this operator
     * @param compiler     compiles functions used by the operator
     * @return {@link Stream}s that statisfy the outputs of this operator
     */
    Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler);

    @Override
    default ResourceUsageEstimate estimateCpuUsage(CardinalityEstimate[] inputCardinalities,
                                                   CardinalityEstimate[] outputCardinalities) {
        LoggerFactory.getLogger(this.getClass()).warn("Using fallback CPU resource function for {}.", this);
        ResourceFunction resourceFunction = ResourceFunction.createFallback(
                inputCardinalities.length,
                outputCardinalities.length);
        return resourceFunction.calculate(inputCardinalities, outputCardinalities);
    }

    @Override
    default ResourceUsageEstimate estimateRamUsage(CardinalityEstimate[] inputCardinalities,
                                                   CardinalityEstimate[] outputCardinalities) {
        LoggerFactory.getLogger(this.getClass()).warn("Using fallback RAM resource function for {}.", this);
        ResourceFunction resourceFunction = ResourceFunction.createFallback(
                inputCardinalities.length,
                outputCardinalities.length);
        return resourceFunction.calculate(inputCardinalities, outputCardinalities);
    }

    @Override
    default ResourceUsageEstimate estimateDiskUsage(CardinalityEstimate[] inputCardinalities,
                                                   CardinalityEstimate[] outputCardinalities) {
        LoggerFactory.getLogger(this.getClass()).warn("Using fallback disk resource function for {}.", this);
        ResourceFunction resourceFunction = ResourceFunction.createFallback(
                inputCardinalities.length,
                outputCardinalities.length);
        return resourceFunction.calculate(inputCardinalities, outputCardinalities);
    }

    @Override
    default ResourceUsageEstimate estimateNetworkUsage(CardinalityEstimate[] inputCardinalities,
                                                   CardinalityEstimate[] outputCardinalities) {
        LoggerFactory.getLogger(this.getClass()).warn("Using fallback network resource function for {}.", this);
        ResourceFunction resourceFunction = ResourceFunction.createFallback(
                inputCardinalities.length,
                outputCardinalities.length);
        return resourceFunction.calculate(inputCardinalities, outputCardinalities);
    }
}
