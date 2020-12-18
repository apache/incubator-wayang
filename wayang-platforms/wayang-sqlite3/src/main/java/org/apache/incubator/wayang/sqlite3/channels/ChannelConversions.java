package org.apache.incubator.wayang.sqlite3.channels;

import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.incubator.wayang.java.channels.StreamChannel;
import org.apache.incubator.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.incubator.wayang.sqlite3.platform.Sqlite3Platform;

import java.util.Collection;
import java.util.Collections;

/**
 * Register for the {@link ChannelConversion}s supported for this platform.
 */
public class ChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            Sqlite3Platform.getInstance().getSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new SqlToStreamOperator(Sqlite3Platform.getInstance())
    );

    public static final Collection<ChannelConversion> ALL = Collections.singleton(
            SQL_TO_STREAM_CONVERSION
    );

}
