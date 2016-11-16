package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    private LoadProfileEstimator loadProfileEstimator;

    public FunctionDescriptor() {
        this(null, null);
    }

    public FunctionDescriptor(LoadEstimator cpuLoadEstimator,
                              LoadEstimator memoryLoadEstimator) {
        this.setLoadEstimators(cpuLoadEstimator, memoryLoadEstimator);
    }

    @SuppressWarnings("unchecked")
    public void setLoadEstimators(LoadEstimator cpuLoadEstimator,
                                  LoadEstimator memoryLoadEstimator) {
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
     * Utility method to retrieve the selectivity of a {@link FunctionDescriptor}
     *
     * @param functionDescriptor either a {@link PredicateDescriptor}, a {@link FlatMapDescriptor}, or a {@link MapPartitionsDescriptor}
     * @return the selectivity
     */
    public static Optional<ProbabilisticDoubleInterval> getSelectivity(FunctionDescriptor functionDescriptor) {
        if (functionDescriptor == null) throw new NullPointerException();
        if (functionDescriptor instanceof PredicateDescriptor) {
            return ((PredicateDescriptor<?>) functionDescriptor).getSelectivity();
        }
        if (functionDescriptor instanceof FlatMapDescriptor) {
            return ((FlatMapDescriptor<?, ?>) functionDescriptor).getSelectivity();
        }
        if (functionDescriptor instanceof MapPartitionsDescriptor) {
            return ((MapPartitionsDescriptor<?, ?>) functionDescriptor).getSelectivity();
        }
        throw new IllegalArgumentException(String.format("Cannot retrieve selectivity of %s.", functionDescriptor));
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

    @FunctionalInterface
    public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

    }

    public interface ExtendedSerializablePredicate<T> extends SerializablePredicate<T>, ExtendedFunction {

    }
}
