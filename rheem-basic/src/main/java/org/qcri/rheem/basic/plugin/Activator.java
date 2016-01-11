package org.qcri.rheem.basic.plugin;

import org.qcri.rheem.basic.mapping.CollocateByMapping;
import org.qcri.rheem.basic.mapping.GlobalReduceMapping;
import org.qcri.rheem.basic.mapping.ReduceByMapping;
import org.qcri.rheem.core.api.RheemContext;

import java.util.Arrays;

/**
 * Activator for the basic Rheem package.
 */
public class Activator {

    public static void activate(RheemContext rheemContext) {
        Arrays.asList(
                new ReduceByMapping(),
                new CollocateByMapping(),
                new GlobalReduceMapping()
        ).stream().forEach(rheemContext::register);
    }

}
