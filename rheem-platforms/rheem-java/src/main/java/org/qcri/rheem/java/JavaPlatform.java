package org.qcri.rheem.java;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.channels.JavaChannelManager;
import org.qcri.rheem.java.mapping.*;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class JavaPlatform extends Platform {

    private static final String PLATFORM_NAME = "Java Streams";

    private final Collection<Mapping> mappings = new LinkedList<>();

    private static JavaPlatform instance = null;

    public static JavaPlatform getInstance() {
        if (instance == null) {
            instance = new JavaPlatform();
        }
        return instance;
    }
    
    private JavaPlatform() {
        super(PLATFORM_NAME);
        this.initializeMappings();
    }

    private void initializeMappings() {
        this.mappings.add(new TextFileSourceToJavaTextFileSourceMapping());
        this.mappings.add(new MapOperatorToJavaMapOperatorMapping());
        this.mappings.add(new ReduceByOperatorToJavaReduceByOperatorMapping());
        this.mappings.add(new JavaCollectionSourceMapping());
        this.mappings.add(new JavaLocalCallbackSinkMapping());
        this.mappings.add(new JavaGlobalReduceOperatorMapping());
        this.mappings.add(new JavaCollocateByOperatorMapping());
        this.mappings.add(new FlatMapToJavaFlatMapMapping());
        this.mappings.add(new CountToJavaCountMapping());
        this.mappings.add(new DistinctToJavaDistinctMapping());
        this.mappings.add(new SortToJavaSortMapping());
        this.mappings.add(new FilterToJavaFilterMapping());
        this.mappings.add(new UnionAllToJavaUnionAllMapping());
        this.mappings.add(new CartesianToJavaCartesianMapping());
        this.mappings.add(new LoopToJavaLoopMapping());
        this.mappings.add(new DoWhileMapping());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public boolean isExecutable() {
        return true;
    }

    @Override
    protected ChannelManager createChannelManager() {
        return new JavaChannelManager(this);
    }

    @Override
    public JavaChannelManager getChannelManager() {
        return (JavaChannelManager) super.getChannelManager();
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JavaExecutor(this, job);
    }
}
