package org.apache.wayang.core.test;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;

/**
 * {@link Platform} implementation for test purposes.
 */
public class DummyPlatform extends Platform {

    private static DummyPlatform INSTANCE;

    private DummyPlatform() {
        super("Dummy Platform", "dummy");
    }

    public static DummyPlatform getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DummyPlatform();
        }
        return INSTANCE;
    }

    @Override
    public void configureDefaults(Configuration configuration) {
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        return new LoadProfileToTimeConverter(null, null, null) {
            @Override
            public TimeEstimate convert(LoadProfile loadProfile) {
                return new TimeEstimate(
                        loadProfile.getCpuUsage().getLowerEstimate(),
                        loadProfile.getCpuUsage().getUpperEstimate(),
                        0.9d
                );
            }
        };
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(0d, 1d);
    }
}
