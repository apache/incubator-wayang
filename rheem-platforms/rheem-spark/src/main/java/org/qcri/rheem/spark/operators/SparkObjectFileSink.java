package org.qcri.rheem.spark.operators;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSource
 */
public class SparkObjectFileSink<T> extends UnarySink<T> implements SparkExecutionOperator {

    private final String targetPath;

    public SparkObjectFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public SparkObjectFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length <= 1;

        final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String targetPath = output.addGivenOrTempPath(this.targetPath, sparkExecutor.getConfiguration());
        RddChannel.Instance input = (RddChannel.Instance) inputs[0];

        input.provideRdd()
                .coalesce(1) // TODO: Remove. This only hotfixes the issue that JavaObjectFileSource reads only a single file.
                .saveAsObjectFile(targetPath);
        LoggerFactory.getLogger(this.getClass()).info("Writing dataset to {}.", targetPath);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSink<>(targetPath, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final String specification = configuration.getStringProperty("rheem.spark.objectfilesink.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }
}
