package org.qcri.rheem.spark.operators;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
        inputs[0].provideRdd().saveAsObjectFile(this.targetPath);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSink<>(this.targetPath, this.getType());
    }

    public String getTargetPath() {
        return this.targetPath;
    }
}
