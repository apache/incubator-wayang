package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    private LoadProfileEstimator loadProfileEstimator;

    public FunctionDescriptor(LoadProfileEstimator loadProfileEstimator) {
        this.setLoadProfileEstimator(loadProfileEstimator);
    }

    public void setLoadProfileEstimator(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
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
     * Updates the {@link LoadProfileEstimator} of this instance.
     *
     * @param cpuEstimator the {@link LoadEstimator} for the CPU load
     * @param ramEstimator the {@link LoadEstimator} for the RAM load
     * @deprecated Use {@link #setLoadProfileEstimator(LoadProfileEstimator)} instead.
     */
    public void setLoadEstimators(LoadEstimator cpuEstimator, LoadEstimator ramEstimator) {
        this.setLoadProfileEstimator(new NestableLoadProfileEstimator(
                cpuEstimator,
                ramEstimator
        ));
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableFunction<Input, Output> extends Function<Input, Output>, Serializable {
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableBiFunction<Input0, Input1, Output> extends BiFunction<Input0, Input1, Output>, Serializable {
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

    /**
     * Decorates the default {@link Consumer} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

    }
    /**
     * Extends a {@link SerializableConsumer} to an {@link ExtendedFunction}.
     */
    public interface ExtendedSerializableConsumer<T> extends SerializableConsumer<T>, ExtendedFunction{

    }
}
