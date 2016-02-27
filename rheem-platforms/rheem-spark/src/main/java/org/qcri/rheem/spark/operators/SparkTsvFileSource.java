package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

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

        final JavaRDD<T> dataQuantaRdd = executor.sc.textFile(this.sourcePath)
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

}
