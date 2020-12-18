package org.apache.incubator.wayang.postgres.channels;

import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.incubator.wayang.java.channels.StreamChannel;
import org.apache.incubator.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.incubator.wayang.postgres.platform.PostgresPlatform;

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
