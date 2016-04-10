package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.BernoulliSampleOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;


/**
 * Spark implementation of the {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperator<Type>
        extends BernoulliSampleOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param predicateDescriptor
     */
    public SparkBernoulliSampleOperator(double fraction, DataSetType type,
                                        PredicateDescriptor<Type> predicateDescriptor) {
        super(fraction, predicateDescriptor, type);
    }

    /**
     * Creates a new instance.
     *
     * @param fraction
     */
    public SparkBernoulliSampleOperator(double fraction, DataSetType type) {
        super(fraction, null, type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final JavaRDD<Type> inputRdd = inputs[0].provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd.sample(false, fraction);

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBernoulliSampleOperator<>(this.fraction, this.getInputType(), this.getPredicateDescriptor());
    }
}
