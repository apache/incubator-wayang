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
