/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.monitor;


import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.wayang.core.util.json.WayangJsonObj;

public class FileMonitor extends Monitor {

    @Override
    public void initialize(Configuration config, String runId, List<Map> initialExecutionPlan) throws IOException {
        this.initialExecutionPlan = initialExecutionPlan;
        this.runId = runId;
        String runsDir = config.getStringProperty(DEFAULT_MONITOR_BASE_URL_PROPERTY_KEY, DEFAULT_MONITOR_BASE_URL);
        final String path = runsDir + "/" + runId;
        this.exPlanUrl = path + "/execplan.json";
        this.progressUrl = path + "/progress.json";

        final FileSystem execplanFile = FileSystems.getFileSystem(exPlanUrl).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(execplanFile.create(exPlanUrl, true))) {
            HashMap<String, Object> jsonPlanMap = new HashMap<>();
            jsonPlanMap.put("stages", initialExecutionPlan);
            jsonPlanMap.put("run_id", runId);
            WayangJsonObj jsonPlan = new WayangJsonObj(jsonPlanMap);

            writer.write(jsonPlan.toString());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        HashMap<String, Integer> initialProgress = new HashMap<>();
        for (Map stage: initialExecutionPlan) {
            for (Map operator: (List<Map>)stage.get("operators")) {
                initialProgress.put((String)operator.get("name"), 0);
            }
        }
        updateProgress(initialProgress);

    }

    @Override
    public void updateProgress(HashMap<String, Integer> partialProgress) throws IOException {
        HashMap<String, Object> progressBar = new HashMap<>();
        Integer overall = 0;
        for (String operatorName : partialProgress.keySet()) {
            this.progress.put(operatorName, partialProgress.get(operatorName));
        }

        for (String operatorName: this.progress.keySet()) {
            overall = overall + this.progress.get(operatorName);
        }

        if (this.progress.size()>0)
            overall = overall/this.progress.size();

        final FileSystem progressFile = FileSystems.getFileSystem(progressUrl).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(progressFile.create(progressUrl, true))) {
            progressBar.put("overall", overall);
            progressBar.put("details", progress);

            WayangJsonObj jsonProgress = new WayangJsonObj(progressBar);
            writer.write(jsonProgress.toString());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
