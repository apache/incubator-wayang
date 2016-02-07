package org.qcri.rheem.java.plugin;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.channels.Channels;
import org.qcri.rheem.java.mapping.*;
import org.qcri.rheem.java.platform.JavaExecutor;

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
        initializeMappings();
    }

    private void initializeMappings() {
        this.mappings.add(new TextFileSourceToJavaTextFileSourceMapping());
        this.mappings.add(new StdoutSinkToJavaStdoutSinkMapping());
        this.mappings.add(new MapOperatorToJavaMapOperatorMapping());
        this.mappings.add(new ReduceByOperatorToJavaReduceByOperatorMapping());
        this.mappings.add(new JavaCollectionSourceMapping());
        this.mappings.add(new JavaLocalCallbackSinkMapping());
        this.mappings.add(new JavaGlobalReduceOperatorMapping());
        this.mappings.add(new JavaCollocateByOperatorMapping());
        this.mappings.add(new JavaCoalesceOperatorMapping());
        this.mappings.add(new FlatMapToJavaFlatMapMapping());
        this.mappings.add(new CountToJavaCountMapping());
        this.mappings.add(new DistinctToJavaDistinctMapping());
        this.mappings.add(new SortToJavaSortMapping());
        this.mappings.add(new FilterToJavaFilterMapping());
        this.mappings.add(new UnionAllToJavaUnionAllMapping());
        this.mappings.add(new CartesianToJavaCartesianMapping());
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
    public Executor.Factory getExecutorFactory() {
        return JavaExecutor.FACTORY;
    }

    @Override
    public <T extends Channel> ChannelInitializer<T> getChannelInitializer(Class<T> channelClass) {
        // Delegate.
        return Channels.getChannelInitializer(channelClass);
    }
}
