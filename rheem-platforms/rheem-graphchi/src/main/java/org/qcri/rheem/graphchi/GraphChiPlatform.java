package org.qcri.rheem.graphchi;

import edu.cmu.graphchi.io.CompressedIO;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;
import org.qcri.rheem.graphchi.mappings.PageRankMapping;

import java.util.Collection;
import java.util.LinkedList;

/**
 * GraphChi {@link Platform} for Rheem.
 */
public class GraphChiPlatform extends Platform {

    public static final String CPU_MHZ_PROPERTY = "rheem.graphchi.cpu.mhz";

    public static final String CORES_PROPERTY = "rheem.graphchi.cores";

    public static final String HDFS_MS_PER_MB_PROPERTY = "rheem.graphchi.hdfs.ms-per-mb";

    private static final String DEFAULT_CONFIG_FILE = "/rheem-graphchi-defaults.properties";

    private static Platform instance;

    private final Collection<Mapping> mappings = new LinkedList<>();

    protected GraphChiPlatform() {
        super("GraphChi");
        this.initialize();
    }

    /**
     * Initializes this instance.
     */
    private void initialize() {
        // Set up.
        CompressedIO.disableCompression();
        GraphChiPlatform.class.getClassLoader().setClassAssertionStatus(
                "edu.cmu.graphchi.preprocessing.FastSharder", false);

        this.initializeConfiguration();
        this.mappings.add(new PageRankMapping());
    }

    private void initializeConfiguration() {
        Configuration.getDefaultConfiguration().load(this.getClass().getResourceAsStream(DEFAULT_CONFIG_FILE));
    }

    public static Platform getInstance() {
        if (instance == null) {
            instance = new GraphChiPlatform();
        }
        return instance;
    }

    @Override
    public void addChannelConversionsTo(ChannelConversionGraph channelConversionGraph) {
        // No ChannelConversions supported so far.
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new GraphChiExecutor(this, job.getConfiguration());
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
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(CPU_MHZ_PROPERTY);
        int numCores = (int) configuration.getLongProperty(CORES_PROPERTY);
        double hdfsMsPerMb = configuration.getDoubleProperty(HDFS_MS_PER_MB_PROPERTY);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }
}
