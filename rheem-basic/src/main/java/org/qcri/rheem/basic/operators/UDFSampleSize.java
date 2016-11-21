package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ExecutionContext;

/**
 * Created by zoi on 11/20/16.
 */
public interface UDFSampleSize {

    /**
     * Called before this instance is actually executed.
     *
     * @param ctx the {@link ExecutionContext}
     */
    void open(ExecutionContext ctx);

    /**
     * Execute this UDF.
     **/
    int apply();
}
