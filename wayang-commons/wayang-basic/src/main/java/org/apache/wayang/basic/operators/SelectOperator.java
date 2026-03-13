package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Row;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.ArrayList;

public class SelectOperator extends UnaryToUnaryOperator<Row, Row> {

    /**
     * About the following property and the properties of new Operators for DF API:
     *
     * When working with DataFrames, user is forced to write untyped expressions instead
     * of hard typed ones. e.g.: ds.filter($"age" > 21) instead of ds.filter(user => user.age > 21).
     * For this reason none of the new Operators will exploit PredicateDescriptor, whose 'core' is a UDF.
     *
     * Regarding SelectOperator, it might have a String (maybe wrapped) that will be exploited
     * by the engine chosen for the actual execution. (it is common for big data
     * engines to have this signature for the select method). Other Operator (e.g. filter)
     * will need an untyped expression instead of PredicateDescriptor
     *
     * The untyped architecture of DF makes the compilation time less prone to find errors.
     * However, big data engines exploit this in order to increase performances. For these reasons, in order to have a proper
     * DF API, it is necessary to create new implementations of existing Operators (e.g. FilterOperator) that will
     * exploit different backends (e.g. currently there is SparkFilterOperator that exploits JavaRDD,
     * while it would be optimal for a DF API to exploit Dataset<Row> (i.e. DataFrame) instead of JavaRDD).
     *
     * Note that both In an Out type is Row (due to DF's untyped architecture)
     */
    protected final ArrayList<String> columns;

    public SelectOperator(ArrayList<String> cols, DataSetType<Row> type) {
        super(type, type, true);
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
