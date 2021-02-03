package org.apache.wayang.tests.platform;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;

import java.util.Collection;
import java.util.Collections;

import static org.mockito.Mockito.mock;

/**
 * Dummy {@link Platform} that does not provide any {@link Mapping}s.
 */
public class MyMadeUpPlatform extends Platform implements Plugin {

    private static MyMadeUpPlatform instance = null;

    public static MyMadeUpPlatform getInstance() {
        if (instance == null) {
            instance = new MyMadeUpPlatform();
        }
        return instance;
    }

    public MyMadeUpPlatform() {
        super("My made up platform", "my_made_up_platform");
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return mock(Executor.Factory.class);
    }

    @Override
    public void configureDefaults(Configuration configuration) {
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singletonList(this);
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return Collections.emptyList();
    }

    @Override
    public void setProperties(Configuration configuration) {
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        throw new RuntimeException("Not yet implemented.");
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        throw new RuntimeException("Not yet implemented.");
    }
}
