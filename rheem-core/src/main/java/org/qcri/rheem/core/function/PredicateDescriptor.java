package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.types.BasicDataUnitType;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * This descriptor pertains to predicates that consume a single data unit.
 *
 * @param <Input>  input type of the transformation function
 */
public class PredicateDescriptor<Input> extends FunctionDescriptor {

    protected final BasicDataUnitType<Input> inputType;

    private final SerializablePredicate<Input> javaImplementation;

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               Class<Input> inputTypeClass) {
        this(javaImplementation, BasicDataUnitType.createBasic(inputTypeClass));
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               Class<Input> inputTypeClass,
                               LoadEstimator cpuLoadEstimator,
                               LoadEstimator ramLoadEstimator) {
        this(javaImplementation, BasicDataUnitType.createBasic(inputTypeClass), cpuLoadEstimator, ramLoadEstimator);
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               BasicDataUnitType inputType) {
        this(javaImplementation, inputType,
                LoadEstimator.createFallback(1, 1),
                LoadEstimator.createFallback(1, 1));
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               BasicDataUnitType inputType,
                               LoadEstimator cpuLoadEstimator,
                               LoadEstimator ramLoadEstimator) {
        super(cpuLoadEstimator, ramLoadEstimator);
        this.javaImplementation = javaImplementation;
        this.inputType = inputType;
    }

    /**
     * This is function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a function that can perform the reduce
     */
    public Predicate<Input> getJavaImplementation() {
        return this.javaImplementation;
    }

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public PredicateDescriptor<Object> unchecked() {
        return (PredicateDescriptor<Object>) this;
    }

    public BasicDataUnitType<Input> getInputType() {
        return this.inputType;
    }

    @FunctionalInterface
    public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

    }

    public interface ExtendedSerializablePredicate<T> extends SerializablePredicate<T>, ExtendedFunction {

    }
}
