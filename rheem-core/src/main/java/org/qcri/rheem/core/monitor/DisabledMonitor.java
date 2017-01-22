package org.qcri.rheem.core.monitor;

import org.qcri.rheem.core.api.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DisabledMonitor extends Monitor {
    @Override
    public void initialize(Configuration config, String runId, List<Map> initialExecutionPlan) throws IOException {

    }

    @Override
    public void updateProgress(HashMap<String, Integer> partialProgress) throws IOException {

    }
}
