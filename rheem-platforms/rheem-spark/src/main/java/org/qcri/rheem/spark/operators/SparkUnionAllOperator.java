package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Optional;

/**
 * Spark implementation of the {@link UnionAllOperator}.
 */
public class SparkUnionAllOperator<Type>
        extends UnionAllOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     */
    public SparkUnionAllOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final JavaRDD<Type> inputRdd0 = inputs[0].provideRdd();
        final JavaRDD<Type> inputRdd1 = inputs[1].provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd0.union(inputRdd1);

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkUnionAllOperator<>(this.getInputType0());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 2000000000L),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                0.3d,
                1000
        );

        return Optional.of(mainEstimator);
    }
}
