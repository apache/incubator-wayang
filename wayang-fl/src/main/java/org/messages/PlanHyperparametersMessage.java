package org.messages;

import org.functions.PlanFunction;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.Map;

public class PlanHyperparametersMessage implements Message{
    private final PlanFunction<Operator, Object, JavaPlanBuilder> function;
    private final Map<String, Object> client_hyperparams;

    public PlanHyperparametersMessage(PlanFunction<Operator, Object, JavaPlanBuilder> function, Map<String, Object> client_hyperparams) {
        this.function = function;
        this.client_hyperparams = client_hyperparams;
    }

    public PlanFunction<Operator, Object, JavaPlanBuilder> getSerializedplan() {
        return function;
    }

    public Map<String, Object> getHyperparams(){return client_hyperparams;}
}
