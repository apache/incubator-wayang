package org.apache.wayang.api.sql.calcite.converter.functions;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.basic.data.Record;

public class SortMapping implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private int index;
    private final int fetch;
    private final int offset;

    public SortMapping(int fetch, int offset) {
        this.fetch = fetch;
        this.offset = offset;
    }

    public Record apply(final Record record) {
        index++;
        return index - 1 > offset && index - 1 < fetch ? record : new Record();
    }
}
