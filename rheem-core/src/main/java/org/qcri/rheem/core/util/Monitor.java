package org.qcri.rheem.core.util;


import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: Rerwrite properly to be more generic, and register properly with application.
public class Monitor {
    private Map Progress;
    private List initialExecutionPlan;
    private String runId;

    public static final String DEFAULT_RUNS_DIR = "file:///var/tmp/rheem/runs";
    public static final String DEFAULT_RUNS_DIR_PROPERTY_KEY = "rheem.basic.runsdir";

    public void initialize(Configuration config, String runId, List initialExecutionPlan) throws IOException {
        this.initialExecutionPlan = initialExecutionPlan;
        this.runId = runId;
        String runsDir = config.getStringProperty(DEFAULT_RUNS_DIR_PROPERTY_KEY, DEFAULT_RUNS_DIR);

        final String path = runsDir + "/" + runId;
        final String exPlanUrl = path + "/execplan.json";
        final String progressUrl = path + "/progress.json";
        final FileSystem execplanFile = FileSystems.getFileSystem(exPlanUrl).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(execplanFile.create(exPlanUrl, true))) {
            HashMap<String, Object> jsonPlanMap = new HashMap<>();
            jsonPlanMap.put("stages", initialExecutionPlan);
            jsonPlanMap.put("run_id", runId);
            JSONObject jsonPlan = new JSONObject(jsonPlanMap);
            writer.write(jsonPlan.toString());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        final FileSystem progressFile = FileSystems.getFileSystem(progressUrl).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(progressFile.create(progressUrl, true))) {
            HashMap<String, Object> progressMap = new HashMap<>();
            progressMap.put("overall", "100");
            JSONObject jsonPlan = new JSONObject(progressMap);
            writer.write(jsonPlan.toString());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }


    }
}
