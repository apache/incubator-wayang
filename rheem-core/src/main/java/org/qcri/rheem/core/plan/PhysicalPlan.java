package org.qcri.rheem.core.plan;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A physical plan consists of a set of {@link Operator}s.
 */
public class PhysicalPlan {

    private final Collection<Operator> sinks = new LinkedList<>();

    public void addSink(Operator sink) {
        if (!sink.isSink()) {
            throw new IllegalArgumentException("Operator is not a sink.");
        }
        this.sinks.add(sink);
    }

    public Collection<Operator> getSinks() {
        return sinks;
    }
}
