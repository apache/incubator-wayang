package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.MapPartitionsDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator takes as input potentially multiple input data quanta and outputs multiple input data quanta.
 * <p>Since Rheem is not a physical execution engine, its notion of partitions is rather loose. Implementors
 * of this operator should guarantee that the partitions are distinct in their data quanta and that all partitions together
 * are complete w.r.t. the data quanta.</p>
 * <p>However, no further assumptions on partitions shall be made, such as: whether partitions can be iterated multiple
 * times; whether partitions can be empty; whether there is a partition on each machine on distributed platforms;
 * or whether partitions have a certain sorting order.</p>
 */
public class MapPartitionsOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final MapPartitionsDescriptor<InputType, OutputType> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public MapPartitionsOperator(FunctionDescriptor.SerializableFunction<Iterable<InputType>, Iterable<OutputType>> function,
                                 Class<InputType> inputTypeClass,
                                 Class<OutputType> outputTypeClass) {
        this(new MapPartitionsDescriptor<>(function, inputTypeClass, outputTypeClass));
    }

    /**
     * Creates a new instance.
     */
    public MapPartitionsOperator(MapPartitionsDescriptor<InputType, OutputType> functionDescriptor) {
        this(functionDescriptor,
                DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType())
        );
    }

    /**
     * Creates a new instance.
     */
    public MapPartitionsOperator(MapPartitionsDescriptor<InputType, OutputType> functionDescriptor,
                                 DataSetType<InputType> inputType,
                                 DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
        this.functionDescriptor = functionDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public MapPartitionsOperator(MapPartitionsOperator<InputType, OutputType> that) {
        super(that);
        this.functionDescriptor = that.getFunctionDescriptor();
    }

    public MapPartitionsDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return this.functionDescriptor;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new MapPartitionsOperator.CardinalityEstimator(configuration));
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    private class CardinalityEstimator implements org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator {

        /**
         * The selectivity of this instance.
         */
        private final ProbabilisticDoubleInterval selectivity;

        private CardinalityEstimator(Configuration configuration) {
            this.selectivity = configuration
                    .getUdfSelectivityProvider()
                    .provideFor(MapPartitionsOperator.this.functionDescriptor);
        }

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            assert MapPartitionsOperator.this.getNumInputs() == inputEstimates.length;
            final CardinalityEstimate inputEstimate = inputEstimates[0];
            return new CardinalityEstimate(
                    (long) (inputEstimate.getLowerEstimate() * this.selectivity.getLowerEstimate()),
                    (long) (inputEstimate.getUpperEstimate() * this.selectivity.getUpperEstimate()),
                    inputEstimate.getCorrectnessProbability() * this.selectivity.getCorrectnessProbability()
            );
        }
    }
}
