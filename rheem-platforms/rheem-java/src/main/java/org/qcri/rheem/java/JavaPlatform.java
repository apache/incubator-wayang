package org.qcri.rheem.java;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.channels.ChannelConversions;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.mapping.*;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class JavaPlatform extends Platform implements Plugin {

    private static final String PLATFORM_NAME = "Java Streams";

    private static final String DEFAULT_CONFIG_FILE = "rheem-java-defaults.properties";

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

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }

    private void initializeMappings() {
        this.mappings.add(new TextFileSourceToJavaTextFileSourceMapping());
        this.mappings.add(new MapOperatorToJavaMapOperatorMapping());
        this.mappings.add(new ReduceByOperatorToJavaReduceByOperatorMapping());
        this.mappings.add(new JavaCollectionSourceMapping());
        this.mappings.add(new JavaLocalCallbackSinkMapping());
        this.mappings.add(new JavaGlobalReduceOperatorMapping());
        this.mappings.add(new JavaCollocateByOperatorMapping());
        this.mappings.add(new GlobalMaterializedGroupToJavaGlobalMaterializedGroupMapping());
        this.mappings.add(new FlatMapToJavaFlatMapMapping());
        this.mappings.add(new CountToJavaCountMapping());
        this.mappings.add(new DistinctToJavaDistinctMapping());
        this.mappings.add(new SortToJavaSortMapping());
        this.mappings.add(new FilterToJavaFilterMapping());
        this.mappings.add(new UnionAllToJavaUnionAllMapping());
        this.mappings.add(new IntersectToJavaIntersectMapping());
        this.mappings.add(new CartesianToJavaCartesianMapping());
        this.mappings.add(new JoinToJavaJoinMapping());
        this.mappings.add(new LoopToJavaLoopMapping());
        this.mappings.add(new DoWhileMapping());
        this.mappings.add(new SampleToJavaSampleMapping());
        this.mappings.add(new ZipWithIdMapping());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singleton(this);
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JavaExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("rheem.java.cpu.mhz");
        int numCores = (int) configuration.getLongProperty("rheem.java.cores");
        double hdfsMsPerMb = configuration.getDoubleProperty("rheem.java.hdfs.ms-per-mb");
        double stretch = configuration.getDoubleProperty("rheem.java.stretch");
        return LoadProfileToTimeConverter.createTopLevelStretching(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate),
                stretch
        );
    }
}
