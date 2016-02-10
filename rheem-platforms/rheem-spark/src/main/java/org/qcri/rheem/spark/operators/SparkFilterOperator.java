package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Spark implementation of the {@link FilterOperator}.
 */
public class SparkFilterOperator<Type>
        extends FilterOperator<Type>
        implements SparkExecutionOperator {


    public static class FilterWrapper<Type> implements Function<Type, Boolean> {

        private Predicate<Type> rheemPredicate;

        public FilterWrapper(Predicate<Type> rheemPredicate) {
            this.rheemPredicate = rheemPredicate;
        }

        @Override
        public Boolean call(Type el) throws Exception {
            return this.rheemPredicate.test(el);
        }
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkFilterOperator(DataSetType<Type> type, Predicate<Type> predicate) {
        super(type, predicate);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputRdds[0];

        final JavaRDD<Type> outputStream = inputStream.filter(new FilterWrapper<>(this.predicate));

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkFilterOperator<>(getInputType(), getFunctionDescriptor());
    }
}
