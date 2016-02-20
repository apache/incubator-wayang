package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;


/**
 * Spark implementation of the {@link DistinctOperator}.
 */
public class SparkDistinctOperator<Type>
        extends DistinctOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkDistinctOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final JavaRDD<Type> inputRdd = inputs[0].<Type>provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd.distinct();

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkDistinctOperator<>(this.getInputType());
    }
}
