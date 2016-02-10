package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Provides a {@link Collection} to a Spark job.
 */
public class SparkCollectionSource<Type> extends CollectionSource<Type> implements SparkExecutionOperator {

    public SparkCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler) {
        Validate.isTrue(inputRdds.length == 0);
        return new JavaRDDLike[] { this.getSC().parallelize(this.getCollectionAsList()) };
    }

    private List<Type> getCollectionAsList() {
        final Collection<Type> collection = this.getCollection();
        if (this.collection instanceof List) {
            return (List<Type>) this.collection;
        }
        return new ArrayList<>(collection);
    }

    @Override
    public ExecutionOperator copy() {
        return null;
    }
}
