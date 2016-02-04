package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.costs.ResourceFunction;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    protected final ResourceFunction cpuResourceFunction;

    protected final ResourceFunction memoryResourceFunction;

    public FunctionDescriptor(ResourceFunction cpuResourceFunction, ResourceFunction memoryResourceFunction) {
        this.cpuResourceFunction = cpuResourceFunction;
        this.memoryResourceFunction = memoryResourceFunction;
    }

}
