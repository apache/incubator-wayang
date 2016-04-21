package org.qcri.rheem.spark.operators;

import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSource
 */
public class SparkObjectFileSink<T> extends UnarySink<T> implements SparkExecutionOperator {

    private final String targetPath;

    public SparkObjectFileSink(String targetPath, DataSetType type) {
        super(type, null);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        inputs[0].provideRdd()
                .repartition(1) // TODO: Remove. This only hotfixes the issue that JavaObjectFileSource reads only a single file.
                .saveAsObjectFile(this.targetPath);
        LoggerFactory.getLogger(this.getClass()).info("Writing dataset to {}.", this.targetPath);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSink<>(this.targetPath, this.getType());
    }

    public String getTargetPath() {
        return this.targetPath;
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: Not measured, instead adapted from SparkTextFileSource.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> 500 * inputCards[0] + 5000000000L),
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> 10 * inputCards[0]),
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> inputCards[0] / 10),
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> inputCards[0] * 10 + 5000000),
                0.19d,
                1000
        );
        return Optional.of(mainEstimator);
    }
}
