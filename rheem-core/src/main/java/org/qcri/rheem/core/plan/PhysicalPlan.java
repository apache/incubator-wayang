package org.qcri.rheem.core.plan;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A physical plan consists of a set of {@link Operator}s.
 */
public class PhysicalPlan {

    private final Collection<Sink> sinks = new LinkedList<>();

    public void addSink(Sink sink) {
        this.sinks.add(sink);
    }

    public Collection<Sink> getSinks() {
        return sinks;
    }
}
