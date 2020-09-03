package io.rheem.rheem.sqlite3.plugin;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.plugin.Plugin;
import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.sqlite3.channels.ChannelConversions;
import io.rheem.rheem.sqlite3.platform.Sqlite3Platform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link Sqlite3Platform}.
 */
public class Sqlite3ConversionPlugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(Sqlite3Platform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
    }
}
