package io.rheem.rheem.sqlite3.channels;

import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.optimizer.channels.DefaultChannelConversion;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.jdbc.operators.SqlToStreamOperator;
import io.rheem.rheem.sqlite3.platform.Sqlite3Platform;

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
