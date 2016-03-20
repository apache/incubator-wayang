package org.qcri.rheem.graphchi;

import edu.cmu.graphchi.io.CompressedIO;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.graphchi.channels.GraphChiChannelManager;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;
import org.qcri.rheem.graphchi.mappings.PageRankMapping;

import java.util.Collection;
import java.util.LinkedList;

/**
 * GraphChi {@link Platform} for Rheem.
 */
public class GraphChiPlatform extends Platform {

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

        this.mappings.add(new PageRankMapping());
    }

    public static Platform getInstance() {
        if (instance == null) {
            instance = new GraphChiPlatform();
        }
        return instance;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return () -> new GraphChiExecutor(this);
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
    protected GraphChiChannelManager createChannelManager() {
        return new GraphChiChannelManager(this);
    }
}
