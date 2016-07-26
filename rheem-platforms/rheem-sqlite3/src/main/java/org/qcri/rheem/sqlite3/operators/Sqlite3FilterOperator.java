package org.qcri.rheem.sqlite3.operators;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;
import org.qcri.rheem.sqlite3.Sqlite3Platform;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the {@link FilterOperator} for the {@link Sqlite3Platform}.
 */
public class Sqlite3FilterOperator<Type> extends JdbcFilterOperator<Type> {

    public Sqlite3FilterOperator(PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        super(predicateDescriptor, typeClass);
    }

    public Sqlite3FilterOperator(FilterOperator<Type> that) {
        super(that);
    }

    public Sqlite3FilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        super(type, predicateDescriptor);
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
