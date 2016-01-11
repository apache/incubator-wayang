package org.qcri.rheem.basic.plugin;

import org.qcri.rheem.basic.mapping.ReduceByMapping;
import org.qcri.rheem.core.api.RheemContext;

/**
 * Activator for the basic Rheem package.
 */
public class Activator {

    public static void activate(RheemContext rheemContext) {
        rheemContext.register(new ReduceByMapping());
    }

}
