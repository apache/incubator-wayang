package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see SparkObjectFileSink
 */
public class SparkTsvFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final String sourcePath;

    public SparkTsvFileSource(DataSetType type) {
        this(null, type);
    }

    public SparkTsvFileSource(String sourcePath, DataSetType type) {
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
        final JavaRDD<String> linesRdd = sparkExecutor.sc.textFile(actualInputPath);
        this.name(linesRdd);
        final JavaRDD<T> dataQuantaRdd = linesRdd
                .map(line -> {
                    // TODO: Important. Enrich type informations to create the correct parser!
                    int tabPos = line.indexOf('\t');
                    return (T) new Tuple2<>(
                            Integer.valueOf(line.substring(0, tabPos)),
                            Float.valueOf(line.substring(tabPos + 1)));
                });
        this.name(dataQuantaRdd);

        output.accept(dataQuantaRdd, sparkExecutor);
    }


    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTsvFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
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

        final String specification = configuration.getStringProperty("rheem.spark.tsvfilesource.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

}
