package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.OptimizationContext;
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
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink
 */
public class SparkObjectFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String sourcePath;

    public SparkObjectFileSource(DataSetType<T> type) {
        this(null, type);
    }

    public SparkObjectFileSource(String sourcePath, DataSetType<T> type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         SparkExecutor sparkExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
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
        this.name(rdd);
        output.accept(rdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: Not measured, instead adapted from SparkTextFileSource.
//        final OptionalLong optionalFileSize;
//        if (this.sourcePath == null) {
//            optionalFileSize = OptionalLong.empty();
//        } else {
//            optionalFileSize = FileSystems.getFileSize(this.sourcePath);
//            if (!optionalFileSize.isPresent()) {
//                LoggerFactory.getLogger(this.getClass()).warn("Could not determine file size for {}.", this.sourcePath);
//            }
//        }

        final String specification = configuration.getStringProperty("rheem.spark.objectfilesource.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
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

    @Override
    public boolean isExecutedEagerly() {
        return false;
    }
}
