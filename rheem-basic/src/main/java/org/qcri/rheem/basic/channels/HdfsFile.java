package org.qcri.rheem.basic.channels;

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

    public HdfsFile(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
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

}
