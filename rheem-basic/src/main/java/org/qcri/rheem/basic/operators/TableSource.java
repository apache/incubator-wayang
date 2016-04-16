package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.basic.data.Record;

/**
 * Created by yidris on 3/10/16.
 */
public class TableSource<T> extends UnarySource<T> {

    private final String tableName;

    public String getTableName() {
        return tableName;
    }

    public TableSource(String tableName, Class<T> typeClass) {
        this(tableName, DataSetType.createDefault(typeClass));
    }

    public TableSource(String tableName, DataSetType<T> type) {
        super(type, null);
        this.tableName = tableName;
    }
}
