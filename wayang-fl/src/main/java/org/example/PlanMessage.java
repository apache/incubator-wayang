package org.example;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.List;
import java.util.Map;

public class PlanMessage implements Message{
    private final TriFunction<Operator, List<Double>, JavaPlanBuilder> function;

    public PlanMessage(TriFunction<Operator, List<Double>, JavaPlanBuilder> function) {
        this.function = function;
    }

    public TriFunction<Operator, List<Double>, JavaPlanBuilder> getSerializedplan() {
        return function;
    }
}
