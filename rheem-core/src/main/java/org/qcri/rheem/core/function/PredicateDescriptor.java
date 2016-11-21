package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.types.BasicDataUnitType;

import java.util.Optional;

/**
 * This descriptor pertains to predicates that consume a single data unit.
 *
 * @param <Input> input type of the transformation function
 */
public class PredicateDescriptor<Input> extends FunctionDescriptor {

    protected final BasicDataUnitType<Input> inputType;

    private final SerializablePredicate<Input> javaImplementation;

    private String sqlImplementation;

    /**
     * The selectivity ({code 0..1}) of this instance or {@code null} if unspecified.
     */
    private ProbabilisticDoubleInterval selectivity;

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               Class<Input> inputTypeClass) {
        this(javaImplementation, inputTypeClass, (ProbabilisticDoubleInterval) null);
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               Class<Input> inputTypeClass,
                               ProbabilisticDoubleInterval selectivity) {
        this(javaImplementation, inputTypeClass, selectivity, null);
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               Class<Input> inputTypeClass,
                               LoadProfileEstimator loadProfileEstimator) {
        this(javaImplementation, inputTypeClass, null, loadProfileEstimator);
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               Class<Input> inputTypeClass,
                               ProbabilisticDoubleInterval selectivity,
                               LoadProfileEstimator loadProfileEstimator) {
        this(javaImplementation, BasicDataUnitType.createBasic(inputTypeClass), selectivity, loadProfileEstimator);
    }

    public PredicateDescriptor(SerializablePredicate<Input> javaImplementation,
                               BasicDataUnitType<Input> inputType,
                               ProbabilisticDoubleInterval selectivity,
                               LoadProfileEstimator loadProfileEstimator) {
        super(loadProfileEstimator);
        this.javaImplementation = javaImplementation;
        this.inputType = inputType;
        this.selectivity = selectivity;
    }

    /**
     * This function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a function that can perform the reduce
     */
    public SerializablePredicate<Input> getJavaImplementation() {
        return this.javaImplementation;
    }

    /**
     * This function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a SQL predicate applicable in a {@code WHERE} clause representing this predicate
     */
    public String getSqlImplementation() {
        return this.sqlImplementation;
    }

    /**
     * This function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @param sqlImplementation a SQL predicate applicable in a {@code WHERE} clause representing this predicate
     */
    public PredicateDescriptor<Input> withSqlImplementation(String sqlImplementation) {
        this.sqlImplementation = sqlImplementation;
        return this;
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

    /**
     * Get the selectivity of this instance.
     *
     * @return an {@link Optional} with the selectivity or an empty one if no selectivity was specified
     */
    public Optional<ProbabilisticDoubleInterval> getSelectivity() {
        return Optional.ofNullable(this.selectivity);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.javaImplementation);
    }
}
