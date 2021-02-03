package org.apache.wayang.core.function;

import org.apache.wayang.core.plan.wayangplan.Operator;

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
