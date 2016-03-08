package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.util.OneTimeExecutable;

/**
 * Enumerator for {@link LoopSubplan}s.
 */
public class LoopEnumerator extends OneTimeExecutable {

    private final OptimizationContext.LoopContext loopContext;

    private LoopEnumeration loopEnumeration;

    public LoopEnumerator(OptimizationContext.LoopContext loopContext) {
        this.loopContext = loopContext;
    }

    public LoopEnumeration enumerate() {
        this.tryExecute();
        return this.loopEnumeration;
    }

    @Override
    protected void doExecute() {
        // Create aggregate iteration contexts.
        OptimizationContext aggregateContext = this.loopContext.createAggregateContext(0, this.loopContext.getIterationContexts().size());
    }
}
