package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;


/**
 * This operator groups both inputs by some key and then matches groups with the same key. If a key appears in only
 * one of the input datasets, then the according group is matched with an empty group.
 */
public class CoGroupOperator<InputType0, InputType1, Key>
        extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<Iterable<InputType0>, Iterable<InputType1>>> {

    private static <InputType0, InputType1> DataSetType<Tuple2<InputType0, InputType1>> createOutputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    protected final TransformationDescriptor<InputType0, Key> keyDescriptor0;

    protected final TransformationDescriptor<InputType1, Key> keyDescriptor1;

    public CoGroupOperator(FunctionDescriptor.SerializableFunction<InputType0, Key> keyExtractor0,
                           FunctionDescriptor.SerializableFunction<InputType1, Key> keyExtractor1,
                           Class<InputType0> input0Class,
                           Class<InputType1> input1Class,
                           Class<Key> keyClass) {
        this(
                new TransformationDescriptor<>(keyExtractor0, input0Class, keyClass),
                new TransformationDescriptor<>(keyExtractor1, input1Class, keyClass)
        );
    }

    public CoGroupOperator(TransformationDescriptor<InputType0, Key> keyDescriptor0,
                           TransformationDescriptor<InputType1, Key> keyDescriptor1) {
        super(DataSetType.createDefault(keyDescriptor0.getInputType()),
                DataSetType.createDefault(keyDescriptor1.getInputType()),
                CoGroupOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
    }
    public CoGroupOperator(TransformationDescriptor<InputType0, Key> keyDescriptor0,
                           TransformationDescriptor<InputType1, Key> keyDescriptor1,
                           DataSetType<InputType0> inputType0,
                           DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1, CoGroupOperator.createOutputDataSetType(), true);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
    }


    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public CoGroupOperator(CoGroupOperator<InputType0, InputType1, Key> that) {
        super(that);
        this.keyDescriptor0 = that.getKeyDescriptor0();
        this.keyDescriptor1 = that.getKeyDescriptor1();
    }

    public TransformationDescriptor<InputType0, Key> getKeyDescriptor0() {
        return this.keyDescriptor0;
    }

    public TransformationDescriptor<InputType1, Key> getKeyDescriptor1() {
        return this.keyDescriptor1;
    }


    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // The current idea: We assume, we have a foreign-key like join
        // TODO: Find a better estimator.
        return Optional.of(new DefaultCardinalityEstimator(
                .5d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> (long) (0.1 * (inputCards[0] + inputCards[1]))
        ));
    }
}
