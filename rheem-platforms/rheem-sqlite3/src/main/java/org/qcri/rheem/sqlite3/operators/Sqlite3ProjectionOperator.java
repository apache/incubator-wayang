package org.qcri.rheem.sqlite3.operators;

import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcProjectionOperator;
import org.qcri.rheem.sqlite3.Sqlite3Platform;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the {@link ProjectionOperator} for the {@link Sqlite3Platform}.
 */
public class Sqlite3ProjectionOperator<Input, Output> extends JdbcProjectionOperator<Input, Output> {

    public Sqlite3ProjectionOperator(ProjectionDescriptor<Input, Output> functionDescriptor) {
        super(functionDescriptor);
    }

    public Sqlite3ProjectionOperator(Class<Input> inputClass, Class<Output> outputClass, Integer... fieldIndexes) {
        super(inputClass, outputClass, fieldIndexes);
    }

    public Sqlite3ProjectionOperator(Class<Input> inputClass, Class<Output> outputClass, String... fieldNames) {
        super(inputClass, outputClass, fieldNames);
    }

    public Sqlite3ProjectionOperator(ProjectionOperator<Input, Output> that) {
        super(that);
    }

    @Override
    public Sqlite3Platform getPlatform() {
        return Sqlite3Platform.getInstance();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(this.getPlatform().sqlQueryChannelDescriptor);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(this.getPlatform().sqlQueryChannelDescriptor);
    }

}
