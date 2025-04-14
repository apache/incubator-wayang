package org.apache.wayang.api.sql.calcite.converter.functions;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.basic.data.Record;

public class JoinKeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private final int index;

    public JoinKeyExtractor(int index) {
        this.index = index;
    }

    public Object apply(final Record record) {
        return record.getField(index);
    }
}
