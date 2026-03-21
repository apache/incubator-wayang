package org.apache.wayang.basic.operators;

import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.basic.data.Record;

import java.util.ArrayList;

public class SelectOperator extends UnaryToUnaryOperator<Record, Record> {

    /**
     * When working with DataFrames, user writes untyped instead of functions.
     * e.g.: ds.filter($"age" > 21) instead of ds.filter(user => user.age > 21).
     * For this reason none of the new Operators will exploit PredicateDescriptor, whose 'core' is a UDF.
     *
     * SelectOperator might exploit Strings as it is common for big data engines
     * to have this signature for the select method.
     *
     * Both In and Out type is [[Record]] (due to DF's untyped architecture)
     */
    protected final ArrayList<String> columns;

    public SelectOperator(ArrayList<String> cols) {
        super(DataSetType.createDefault(org.apache.wayang.basic.data.Record.class), DataSetType.createDefault(org.apache.wayang.basic.data.Record.class), true);
        this.columns = cols;
    }

    public SelectOperator(SelectOperator that) {
        super(that);
        this.columns = that.getColumns();
    }

    private ArrayList<String> getColumns() {
        return this.columns;
    }

    /**
     Obviously, this class lacks a region for cardinality estimation.
     */

}
