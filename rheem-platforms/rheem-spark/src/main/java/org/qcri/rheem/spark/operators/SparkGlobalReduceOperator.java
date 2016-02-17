package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link GlobalReduceOperator}.
 */
public class SparkGlobalReduceOperator<Type>
        extends GlobalReduceOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public SparkGlobalReduceOperator(DataSetType<Type> type,
                                     ReduceDescriptor<Type> reduceDescriptor) {
        super(type, reduceDescriptor);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function2<Type, Type, Type> reduceFunction = compiler.compile(this.reduceDescriptor, this, inputs);

        final JavaRDD<Type> inputRdd = inputs[0].provideRdd();
        List<Type> outputList = Collections.singletonList(inputRdd.reduce(reduceFunction));
        // FIXME: This is likely inefficient. The SparkExecutor should be capable of handling these primitive values and put them into RDDs on demand only. However, let's see if this is really a problem.
        JavaRDD<Type> outputRdd = sparkExecutor.sc.parallelize(outputList);

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkGlobalReduceOperator<>(this.getInputType(), this.getReduceDescriptor());
    }
}
