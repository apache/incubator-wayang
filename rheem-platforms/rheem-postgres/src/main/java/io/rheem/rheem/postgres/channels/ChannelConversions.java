package io.rheem.rheem.postgres.channels;

import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.optimizer.channels.DefaultChannelConversion;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.jdbc.operators.SqlToStreamOperator;
import io.rheem.rheem.postgres.platform.PostgresPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Register for the {@link ChannelConversion}s supported for this platform.
 */
public class ChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            PostgresPlatform.getInstance().getSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new SqlToStreamOperator(PostgresPlatform.getInstance())
    );

    public static final Collection<ChannelConversion> ALL = Collections.singleton(
            SQL_TO_STREAM_CONVERSION
    );

}
