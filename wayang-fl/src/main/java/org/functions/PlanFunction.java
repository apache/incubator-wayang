package org.functions;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.io.Serializable;
import java.util.Map;


// Functional interface for two arguments
@FunctionalInterface
public interface PlanFunction extends Serializable {
    Operator apply(Object a, JavaPlanBuilder b, Map<String, Object> c);
}