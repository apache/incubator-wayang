package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see SparkObjectFileSink
 */
public class SparkTsvFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final String sourcePath;

    public SparkTsvFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor executor) {
        assert outputs.length == this.getNumOutputs();

        final String actualInputPath = FileSystems.findActualSingleInputPath(this.sourcePath);
        final JavaRDD<T> dataQuantaRdd = executor.sc.textFile(actualInputPath)
                .map(line -> {
                    // TODO: Important. Enrich type informations to create the correct parser!
                    int tabPos = line.indexOf('\t');
                    return (T) new Tuple2<>(
                            Integer.valueOf(line.substring(0, tabPos)),
                            Float.valueOf(line.substring(tabPos + 1)));
                });

        outputs[0].acceptRdd(dataQuantaRdd);
    }


    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTsvFileSource<>(this.sourcePath, this.getType());
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
