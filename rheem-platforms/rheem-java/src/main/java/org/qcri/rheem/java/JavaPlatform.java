package org.qcri.rheem.java;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.channels.ChannelConversions;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.mapping.*;

import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class JavaPlatform extends Platform {

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
        this.initializeConfiguration();
        this.initializeMappings();
    }

    private void initializeConfiguration() {
        Configuration.getDefaultConfiguration().load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
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
        this.mappings.add(new CartesianToJavaCartesianMapping());
        this.mappings.add(new LoopToJavaLoopMapping());
        this.mappings.add(new DoWhileMapping());
        this.mappings.add(new SampleToJavaSampleMapping());
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
    public void addChannelConversionsTo(ChannelConversionGraph channelConversionGraph) {
        ChannelConversions.ALL.forEach(channelConversionGraph::add);
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
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }
}
