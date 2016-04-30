package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink
 */
public class SparkObjectFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String sourcePath;

    public SparkObjectFileSource(DataSetType type) {
        this(null, type);
    }

    public SparkObjectFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        final String sourcePath;
        if (this.sourcePath != null) {
            assert inputs.length == 0;
            sourcePath = this.sourcePath;
        } else {
            FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            sourcePath = input.getSinglePath();
        }
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final String actualInputPath = FileSystems.findActualSingleInputPath(sourcePath);
        final JavaRDD<Object> rdd = sparkExecutor.sc.objectFile(actualInputPath);
        output.accept(rdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: Not measured, instead adapted from SparkTextFileSource.
        final OptionalLong optionalFileSize = this.sourcePath == null ? OptionalLong.empty() : FileSystems.getFileSize(this.sourcePath);
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

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }
}
