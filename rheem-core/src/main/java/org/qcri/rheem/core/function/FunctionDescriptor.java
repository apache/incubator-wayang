package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    private LoadProfileEstimator loadProfileEstimator;

    public FunctionDescriptor() {
        this(null, null);
    }

    public FunctionDescriptor(LoadEstimator cpuLoadEstimator, LoadEstimator memoryLoadEstimator) {
        this.setLoadEstimators(cpuLoadEstimator, memoryLoadEstimator);
    }

    public void setLoadEstimators(LoadEstimator cpuLoadEstimator, LoadEstimator memoryLoadEstimator) {
        if (cpuLoadEstimator == null && memoryLoadEstimator == null) {
            this.loadProfileEstimator = null;
        } else {
            this.loadProfileEstimator = new NestableLoadProfileEstimator(
                    cpuLoadEstimator == null ?
                            LoadEstimator.createFallback(LoadEstimator.UNSPECIFIED_NUM_SLOTS, LoadEstimator.UNSPECIFIED_NUM_SLOTS) :
                            cpuLoadEstimator,
                    memoryLoadEstimator == null ?
                            LoadEstimator.createFallback(LoadEstimator.UNSPECIFIED_NUM_SLOTS, LoadEstimator.UNSPECIFIED_NUM_SLOTS) :
                            memoryLoadEstimator
            );
        }
    }

    public Optional<LoadProfileEstimator> getLoadProfileEstimator() {
        return Optional.ofNullable(this.loadProfileEstimator);
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableFunction<Input, Output> extends Function<Input, Output>, Serializable {
    }


    /**
     * Extends a {@link SerializableFunction} to an {@link ExtendedFunction}.
     */
    public interface ExtendedSerializableFunction<Input, Output> extends SerializableFunction<Input, Output>, ExtendedFunction {
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableBinaryOperator<Type> extends BinaryOperator<Type>, Serializable {
    }

    /**
     * Extends a {@link SerializableBinaryOperator} to an {@link ExtendedFunction}.
     */
    public interface ExtendedSerializableBinaryOperator<Type> extends SerializableBinaryOperator<Type>, ExtendedFunction {
    }
}
