package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.basic.data.Record;

/**
 * Created by yidris on 3/10/16.
 */
public class TableSource extends UnarySource {

    private final String tableName;

    public String getTableName() {
        return tableName;
    }

    public TableSource(String tableName) {
        super(DataSetType.createDefault(String.class), null);
        this.tableName = tableName;
    }
}
