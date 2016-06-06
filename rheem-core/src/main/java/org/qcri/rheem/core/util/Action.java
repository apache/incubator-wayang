package org.qcri.rheem.core.util;

import org.slf4j.LoggerFactory;

/**
 * This interface represents any piece of code that takes no input and produces no output but may fail.
 */
@FunctionalInterface
public interface Action {

    /**
     * Perform this action.
     *
     * @throws Throwable in case anything goes wrong
     */
    void execute() throws Throwable;

    /**
     * Performs this actionl. If any error occurs, it will be merely logged.
     */
    default void executeSafe() {
        try {
            this.execute();
        } catch (Throwable t) {
            LoggerFactory.getLogger(Action.class).error("Execution failed.", t);
        }
    }
}

