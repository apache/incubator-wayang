package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

public class KMeansOperator extends UnaryToUnaryOperator<double[], Tuple2<double[], Integer>> {
    // TODO other parameters
    protected int k;

    public KMeansOperator(int k) {
        super(DataSetType.createDefaultUnchecked(double[].class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false);
        this.k = k;
    }

    public KMeansOperator(KMeansOperator that) {
        super(that);
        this.k = that.k;
    }

    public int getK() {
        return k;
    }

    // TODO support fit and transform

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        // TODO
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}
