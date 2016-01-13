package org.qcri.rheem.core.plan;

/**
 * Visitor (as in the Visitor Pattern) for {@link PhysicalPlan}s.
 */
public interface PlanVisitor {

    /**
     * todo
     * @param operatorAlternative
     */
    void visit(OperatorAlternative operatorAlternative);

    void visit(Subplan subplan);

    /** todo */
    void visit(ActualOperator operator);

}
