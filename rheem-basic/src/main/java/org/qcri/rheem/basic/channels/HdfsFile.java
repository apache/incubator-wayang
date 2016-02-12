package org.qcri.rheem.basic.channels;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;

import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link Channel} that is realized via file(s) in an HDFS.
 * <p>TODO: Frameworks, such as Spark and Flink, usually produce more than one output file. We need to figure out
 * how to account for this appropriately, once we have those frameworks in place.</p>
 */
public class HdfsFile extends Channel {

    private Collection<String> paths = new LinkedList<>();

    public HdfsFile(ExecutionTask producer, int outputIndex, CardinalityEstimate cardinalityEstimate) {
        super(producer, outputIndex, cardinalityEstimate);
    }

    public void addPath(String path) {
        this.paths.add(path);
    }

    public Collection<String> getPaths() {
        return this.paths;
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s%s", this.getClass().getSimpleName(), this.paths);
    }
}
