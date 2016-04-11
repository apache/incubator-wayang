package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;


public class ProjectionOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final ProjectionDescriptor<InputType, OutputType> functionDescriptor;

    public Boolean isProjectByIndexes() {
        return functionDescriptor.isProjectByIndexes();
    }

    /**
     * Creates a new instance.
     */
    public ProjectionOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                              String... fieldNames) {
        this(new ProjectionDescriptor<InputType, OutputType>(inputTypeClass, outputTypeClass, fieldNames));
    }

    public ProjectionOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                              Integer... fieldIndexes) {
        this(new ProjectionDescriptor<InputType, OutputType>(inputTypeClass, outputTypeClass, fieldIndexes));
    }

    /**
     * Creates a new instance.
     */
    public ProjectionOperator(ProjectionDescriptor<InputType, OutputType> functionDescriptor) {
        this(functionDescriptor,
                DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType()));
    }

    /**
     * Creates a new instance.
     */
    public ProjectionOperator(ProjectionDescriptor<InputType, OutputType> functionDescriptor,
                              DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(inputType, outputType, true, null);
        this.functionDescriptor = functionDescriptor;
    }

    public ProjectionDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return this.functionDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }
}
