package io.rheem.rheem.basic;

import io.rheem.rheem.basic.plugin.RheemBasic;
import io.rheem.rheem.basic.plugin.RheemBasicGraph;

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
