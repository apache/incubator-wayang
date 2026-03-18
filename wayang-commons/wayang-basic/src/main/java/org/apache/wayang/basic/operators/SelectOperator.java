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
     * engines to have this signature for the select method). Also other Operators (e.g. filter)
     * will need an untyped expression instead of PredicateDescriptor.
     *
     * The untyped architecture of DF makes the compilation time less prone to find errors.
     * However, big data engines exploit this in order to increase performances
     * (see Predicate Pushdown). For these reasons, in order to have a proper
     * DF API, it is necessary to create new versions of existing Operators that will
     * be based on untyped expressions instead of UDF and will be able to properly exploit
     * the most performative backends. An example:
     * SparkFilterOperator extends FilterOperator whose core is an udf and exploits JavaRDD,
     * instead Spark_Df_FilterOperator extends Filter_Df_Operator whose core is an untyped expression
     * and exploits Spark Dataframe.
     *
     * Note that both In an Out type is Row (due to DF's untyped architecture)
     */
    protected final ArrayList<String> columns;

    public SelectOperator(ArrayList<String> cols) {
        super(DataSetType.createDefault(Row.class), DataSetType.createDefault(Row.class), true);
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
