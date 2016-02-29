package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * {@link Operator} for the PageRank algorithm.
 */
public class PageRankOperator extends UnaryToUnaryOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Float>> {

    protected final int numIterations;

    /**
     * Creates a new instance.

     */
    public PageRankOperator(int numIterations) {
        super(DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false, null);
        this.numIterations = numIterations;
    }

    public int getNumIterations() {
        return numIterations;
    }

}
