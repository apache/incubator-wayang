package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.HdfsFileInitializer;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see SparkObjectFileSource
 */
public class SparkTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements SparkExecutionOperator {

    private HdfsFileInitializer.Executor outputChannelExecutor;

    private final String targetPath;

    public SparkTsvFileSink(String targetPath, DataSetType type) {
        super(type, null);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
                String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor executor) {
        assert inputs.length == this.getNumInputs();

        final JavaRDD<Object> rdd = inputs[0].provideRdd();
        rdd
                .map(dataQuantum -> {
                    // TODO: Once there are more tuple types, make this generic.
                    @SuppressWarnings("unchecked")
                    Tuple2<Object, Object> tuple2 = (Tuple2<Object, Object>) dataQuantum;
                    return String.valueOf(tuple2.field0) + '\t' + String.valueOf(tuple2.field1);
                })
                .saveAsTextFile(this.targetPath);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTsvFileSink<>(this.targetPath, this.getType());
    }


}
