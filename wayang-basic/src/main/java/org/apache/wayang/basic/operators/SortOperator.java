package org.apache.incubator.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.function.FunctionDescriptor;
import org.apache.incubator.wayang.core.function.TransformationDescriptor;
import org.apache.incubator.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.incubator.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.incubator.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.incubator.wayang.core.types.DataSetType;

import java.util.Optional;


/**
 * This operator sorts the elements in this dataset.
 */
public class SortOperator<Type, Key> extends UnaryToUnaryOperator<Type, Type> {

    protected final TransformationDescriptor<Type, Key> keyDescriptor;

    /**
     * Creates a new instance.
     *
     */
    public SortOperator(FunctionDescriptor.SerializableFunction<Type, Key> keyExtractor, Class<Type> typeClass, Class<Key> keyClass) {
        this(new TransformationDescriptor<>(keyExtractor, typeClass, keyClass));
    }


    /**
     * Creates a new instance.
     *
     * @param keyDescriptor sort key extractor
     */
    public SortOperator(TransformationDescriptor<Type, Key> keyDescriptor) {
        super(DataSetType.createDefault(keyDescriptor.getInputType()),
                DataSetType.createDefault(keyDescriptor.getInputType()),
                false);
        this.keyDescriptor = keyDescriptor;
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public SortOperator(TransformationDescriptor<Type, Key> keyDescriptor, DataSetType<Type> type) {
        super(type, type, false);
        this.keyDescriptor = keyDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SortOperator(SortOperator<Type, Key> that) {
        super(that);
        this.keyDescriptor = that.getKeyDescriptor();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(), inputCards -> inputCards[0]));
    }

}
