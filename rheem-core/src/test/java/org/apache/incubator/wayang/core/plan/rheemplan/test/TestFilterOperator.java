package io.rheem.rheem.core.plan.rheemplan.test;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.optimizer.cardinality.CardinalityEstimator;
import io.rheem.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import io.rheem.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.types.DataUnitType;

import java.util.Optional;

/**
 * Test operator that exposes filter-like behavior.
 */
public class TestFilterOperator<InputType> extends UnaryToUnaryOperator<InputType, InputType> {

    private double selectivity = 0.7d;

    /**
     * Creates a new instance.
     */
    public TestFilterOperator(DataSetType<InputType> inputType) {
        super(inputType, inputType, true);
    }

    public TestFilterOperator(Class<InputType> inputTypeClass) {
        this(DataSetType.createDefault(DataUnitType.createBasic(inputTypeClass)));
    }


    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex,
                                                                     Configuration configuration) {
        assert outputIndex == 0;
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, true, cards -> Math.round(this.selectivity * cards[0])));
    }

    @Override
    public boolean isSupportingBroadcastInputs() {
        return true;
    }

    public double getSelectivity() {
        return this.selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }
}
