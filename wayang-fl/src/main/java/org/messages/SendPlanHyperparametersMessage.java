package org.messages;

import org.functions.PlanFunction;

import java.util.Map;

public class SendPlanHyperparametersMessage implements Message{
    private final PlanFunction plan;
    private final Map<String, Object> client_hyperparams;

    public SendPlanHyperparametersMessage(PlanFunction plan, Map<String, Object> client_hyperparams){
        this.plan = plan;
        this.client_hyperparams = client_hyperparams;
    }

    public PlanFunction getPlan(){
        return plan;
    }

    public Map<String, Object> getClient_hyperparams(){
        return client_hyperparams;
    }
}
