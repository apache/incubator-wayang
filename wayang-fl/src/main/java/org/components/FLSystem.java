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

package org.components;

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorSystem;
import org.client.Client;
import org.server.Server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FLSystem {
    private final Server server;
    private List<Client> clients = new ArrayList<>();
    private Map<String, FLJob> job_ids = new HashMap<>();
    public FLSystem(String server_name, String server_url,
                    List<String> client_names, List<String> client_urls){
        this.server = new Server(server_url, server_name);
        for(int i = 0; i < client_names.size(); i++){
            clients.add(new Client(client_urls.get(i), client_names.get(i)));
        }
    }

    public String registerFLJob(FLJob job, Config config){
        String job_id = Long.toString(System.currentTimeMillis());
        job.setJobId(job_id);
        job_ids.put(job_id, job);
        job.startFLServer(server, config);
        job.initiateHandshake();

        return job_id;
    }

    public void startFLJob(String job_id){
        FLJob job = job_ids.get(job_id);
        job.sendPlanHyperparameters();
        while(job.checkCriterion()){
            Object aggregatedResult = job.runIteration();
            job.updateState(aggregatedResult);
        }
    }

    public Object getFLJobResult(String job_id){
        FLJob job = job_ids.get(job_id);
        return job.getFinalOperand();
    }
}
