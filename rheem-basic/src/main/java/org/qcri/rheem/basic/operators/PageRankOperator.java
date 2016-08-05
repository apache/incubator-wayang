package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * {@link Operator} for the PageRank algorithm. It takes as input a list of directed edges, whereby each edge
 * is represented as {@code (source vertex ID, target vertex ID)} tuple. Its output are the page ranks, codified
 * as {@code (vertex ID, page rank)} tuples.
 */
public class PageRankOperator extends UnaryToUnaryOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Float>> {

    protected final int numIterations;

    /**
     * Creates a new instance.
     *
     * @param numIterations the number of PageRank iterations that this instance should perform
     */
    public PageRankOperator(int numIterations) {
        super(DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false);
        this.numIterations = numIterations;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PageRankOperator(PageRankOperator that) {
        super(that);
        this.numIterations = that.getNumIterations();
    }

    public int getNumIterations() {
        return numIterations;
    }

}
