package org.qcri.rheem.core.function;

import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;

/**
 * While a function is executed on a certain {@link Platform}, allows access to some information of the context in
 * which the function is being executed.
 */
public interface ExecutionContext {

    /**
     * Accesses a broadcast.
     *
     * @param name name of the broadcast
     * @param <T>  type of the broadcast
     * @return the broadcast
     */
    <T> Collection<T> getBroadcast(String name);

    /**
     * If this instance reflects the state of execution inside of a {@link LoopSubplan}, then retrieve the
     * number of the current iteration.
     *
     * @return the iteration number, start at {@code 0}, or {@code -1} if there is no surrounding {@link LoopSubplan}
     */
    int getCurrentIteration();

}
