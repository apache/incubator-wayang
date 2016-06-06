package org.qcri.rheem.core.util;

/**
 * Utilities to perform actions.
 */
public class Actions {

    /**
     * Executes an {@link Action}, catching any error.
     *
     * @param action to be executed
     */
    public static void doSafe(Action action) {
        action.executeSafe();
    }

}
