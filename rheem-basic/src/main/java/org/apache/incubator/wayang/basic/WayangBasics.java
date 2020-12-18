package org.apache.incubator.wayang.basic;

import org.apache.incubator.wayang.basic.plugin.WayangBasic;
import org.apache.incubator.wayang.basic.plugin.WayangBasicGraph;

/**
 * Register for plugins in the module.
 */
public class WayangBasics {

    private static final WayangBasic DEFAULT_PLUGIN = new WayangBasic();

    private static final WayangBasicGraph GRAPH_PLUGIN = new WayangBasicGraph();

    public static WayangBasic defaultPlugin() {
        return DEFAULT_PLUGIN;
    }

    public static WayangBasicGraph graphPlugin() {
        return GRAPH_PLUGIN;
    }

}
