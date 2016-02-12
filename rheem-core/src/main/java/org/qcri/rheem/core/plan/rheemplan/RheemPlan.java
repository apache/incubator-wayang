package org.qcri.rheem.core.plan.rheemplan;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A Rheem plan consists of a set of {@link Operator}s.
 */
public class RheemPlan {

    private final Collection<Operator> sinks = new LinkedList<>();

    public void addSink(Operator sink) {
        if (!sink.isSink()) {
            throw new IllegalArgumentException("Operator is not a sink.");
        }
        this.sinks.add(sink);
    }

    public Collection<Operator> getSinks() {
        return this.sinks;
    }

    /**
     * Find the source {@link Operator}s that are reachable from the sinks.
     * @return the reachable sources, only top-level operators are considered
     * @see #getSinks()
     */
    public Collection<Operator> collectReachableTopLevelSources() {
        return new PlanTraversal(true, false).traverse(this.sinks).getTraversedNodesWith(Operator::isSource);
    }
}
