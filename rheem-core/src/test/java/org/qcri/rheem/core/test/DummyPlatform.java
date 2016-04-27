package org.qcri.rheem.core.test;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TODO
 */
public class DummyPlatform extends Platform {

    private static DummyPlatform INSTANCE;

    private DummyPlatform() {
        super("Dummy Platform");
    }

    public static DummyPlatform getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DummyPlatform();
        }
        return INSTANCE;
    }

    @Override
    protected void registerChannelConversions(Configuration configuration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Mapping> getMappings() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExecutable() {
        return true;
    }

    @Override
    protected ChannelManager createChannelManager() {
        return new ChannelManager() {
            @Override
            public ChannelInitializer getChannelInitializer(ChannelDescriptor channelDescriptor) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Collection<ChannelConversion> getChannelConversions() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean exchangeWithInterstageCapable(Channel channel) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelDescriptor getInternalChannelDescriptor(boolean isRequestReusable) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<ChannelDescriptor, Channel> setUpSourceSide(Junction junction, List<ChannelDescriptor> preferredChannelDescriptors, OptimizationContext optimizationContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setUpTargetSide(Junction junction, int targetIndex, Channel externalChannel, OptimizationContext optimizationContext) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        return null;
    }
}
