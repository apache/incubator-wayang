package org.qcri.rheem.core.function;

import org.qcri.rheem.core.plan.rheemplan.Operator;

/**
 * Used to enrich regular functions with additional life-cycle methods of {@link Operator}s.
 */
public interface ExtendedFunction {

    /**
     * Called before this instance is actually executed.
     *
     * @param ctx the {@link ExecutionContext}
     */
    void open(ExecutionContext ctx);

}
