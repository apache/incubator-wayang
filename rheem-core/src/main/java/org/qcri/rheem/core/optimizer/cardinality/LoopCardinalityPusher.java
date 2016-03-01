package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;

/**
 * {@link CardinalityPusher} implementation for {@link LoopSubplan}s.
 */
public class LoopCardinalityPusher extends CardinalityPusher {

    public LoopCardinalityPusher(LoopSubplan loopSubplan) {
//        new SubplanCardinalityPusher()
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {

    }

}
