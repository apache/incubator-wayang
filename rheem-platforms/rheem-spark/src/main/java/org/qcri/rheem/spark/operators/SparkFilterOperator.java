package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

/**
 * Spark implementation of the {@link FilterOperator}.
 */
public class SparkFilterOperator<Type>
        extends FilterOperator<Type>
        implements SparkExecutionOperator {



    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkFilterOperator(DataSetType<Type> type, PredicateDescriptor<Type> predicate) {
        super(type, predicate);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<Type, Boolean> filterFunction = compiler.compile(this.predicateDescriptor, this, inputs);

        final JavaRDD<Type> inputRdd = inputs[0].<Type>provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd.filter(filterFunction);
        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkFilterOperator<>(this.getInputType(), this.getPredicateDescriptor());
    }
}
