package org.qcri.rheem.graphchi;

import edu.cmu.graphchi.io.CompressedIO;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.graphchi.channels.ChannelManager;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;

import java.util.Collection;

/**
 * GraphChi {@link Platform} for Rheem.
 */
public class GraphChiPlatform extends Platform {

    private static Platform instance;

    protected GraphChiPlatform() {
        super("GraphChi");
        CompressedIO.disableCompression();
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
        return null;
    }

    @Override
    public boolean isExecutable() {
        return true;
    }

    @Override
    protected ChannelManager createChannelManager() {
        return new ChannelManager(this);
    }
}
