package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink
 */
public class SparkObjectFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String sourcePath;

    public SparkObjectFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert outputs.length == this.getNumOutputs();

        final String actualInputPath = FileSystems.findActualSingleInputPath(this.sourcePath);
        final JavaRDD<Object> rdd = sparkExecutor.sc.objectFile(actualInputPath);
        outputs[0].acceptRdd(rdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: Not measured, instead adapted from SparkTextFileSource.
        final OptionalLong optionalFileSize = FileSystems.getFileSize(this.sourcePath);
        if (!optionalFileSize.isPresent()) {
            LoggerFactory.getLogger(this.getClass()).warn("Could not determine file size for {}.", this.sourcePath);
        }

        final NestableLoadProfileEstimator mainEstimator;
        if (optionalFileSize.isPresent()) {
            mainEstimator = new NestableLoadProfileEstimator(
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 700 * outputCards[0] + 5000000000L),
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> optionalFileSize.getAsLong()),
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> outputCards[0] / 10),
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> outputCards[0] * 10 + 5000000),
                    0.19d,
                    1000
            );
        } else {
            mainEstimator = new NestableLoadProfileEstimator(
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 500 * outputCards[0] + 5000000000L),
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 10 * outputCards[0]),
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> outputCards[0] / 10),
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> outputCards[0] * 10 + 5000000),
                    0.19d,
                    1000
            );
        }
        return Optional.of(mainEstimator);
    }
}
