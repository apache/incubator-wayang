package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * A map operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, and pushes that element to the output slot.
 */
public class MapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final TransformationDescriptor<InputType, OutputType> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public MapOperator(FunctionDescriptor.SerializableFunction<InputType, OutputType> function,
                       Class<InputType> inputTypeClass,
                       Class<OutputType> outputTypeClass) {
        this(new TransformationDescriptor<>(function, inputTypeClass, outputTypeClass));
    }

    /**
     * Creates a new instance.
     */
    public MapOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        this(functionDescriptor,
                DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType()));
    }

    /**
     * Creates a new instance.
     */
    public MapOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor, DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
        this.functionDescriptor = functionDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public MapOperator(MapOperator<InputType, OutputType> that) {
        super(that);
        this.functionDescriptor = that.getFunctionDescriptor();
    }

    /**
     * Creates a new instance that projects the given fields.
     *
     * @param fieldNames the field names for the projected fields
     * @return the new instance
     */
    public static <Input, Output> MapOperator<Input, Output> createProjection(
            Class<Input> inputClass,
            Class<Output> outputClass,
            String... fieldNames) {
        return new MapOperator<>(new ProjectionDescriptor<>(inputClass, outputClass, fieldNames));
    }

    /**
     * Creates a new instance that projects the given fields of {@link Record}s.
     *
     * @param fieldNames the field names for the projected fields
     * @return the new instance
     */
    public static MapOperator<Record, Record> createProjection(
            RecordType inputType,
            String... fieldNames) {
        return new MapOperator<>(ProjectionDescriptor.createForRecords(inputType, fieldNames));
    }

    public TransformationDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return this.functionDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }
}
