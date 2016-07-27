package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

/**
 * {@link UnarySource} that provides the tuples from a database table.
 */
public abstract class TableSource<T> extends UnarySource<T> {

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

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TableSource(TableSource that) {
        super(that);
        this.tableName = that.getTableName();
    }

    // TODO: Provide a cardinality estimator.
}
