package org.qcri.rheem.basic;

import org.qcri.rheem.basic.plugin.RheemBasic;
import org.qcri.rheem.basic.plugin.RheemBasicGraph;

/**
 * Register for plugins in the module.
 */
public class RheemBasics {

    private static final RheemBasic DEFAULT_PLUGIN = new RheemBasic();

    private static final RheemBasicGraph GRAPH_PLUGIN = new RheemBasicGraph();

    public static RheemBasic defaultPlugin() {
        return DEFAULT_PLUGIN;
    }

    public static RheemBasicGraph graphPlugin() {
        return GRAPH_PLUGIN;
    }

}
