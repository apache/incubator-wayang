package org.qcri.rheem.core.monitor;

import org.qcri.rheem.core.api.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Monitor {

    protected HashMap<String, Integer> progress = new HashMap<>();
    protected List<Map> initialExecutionPlan;
    protected String exPlanUrl;
    protected String progressUrl;
    protected String runId;

    public static Boolean isEnabled(Configuration config) {
        return config.getBooleanProperty(MONITOR_ENABLED_PROPERTY_KEY, false);
    }
    public abstract void initialize(Configuration config, String runId, List<Map> initialExecutionPlan) throws IOException;

    public abstract void updateProgress(HashMap<String, Integer> partialProgress) throws IOException;

    public static final String DEFAULT_MONITOR_BASE_URL = "file:///var/tmp/rheem/runs";
    public static final String DEFAULT_MONITOR_BASE_URL_PROPERTY_KEY = "rheem.core.monitor.baseurl";
    public static final String MONITOR_ENABLED_PROPERTY_KEY = "rheem.core.monitor.enabled";
}
