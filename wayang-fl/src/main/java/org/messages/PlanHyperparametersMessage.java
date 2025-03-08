package org.messages;

import org.functions.PlanFunction;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.Map;

public class PlanHyperparametersMessage implements Message{
    private final PlanFunction function;
    private final Map<String, Object> client_hyperparams;

    public PlanHyperparametersMessage(PlanFunction function, Map<String, Object> client_hyperparams) {
        this.function = function;
        this.client_hyperparams = client_hyperparams;
    }

    public PlanFunction getSerializedplan() {
        return function;
    }

    public Map<String, Object> getHyperparams(){return client_hyperparams;}
}
