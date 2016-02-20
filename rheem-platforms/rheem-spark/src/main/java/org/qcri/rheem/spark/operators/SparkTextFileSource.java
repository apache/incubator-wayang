package org.qcri.rheem.spark.operators;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Collection;

/**
 * Provides a {@link Collection} to a Spark job.
 */
public class SparkTextFileSource extends TextFileSource implements SparkExecutionOperator {

    public SparkTextFileSource(String inputUrl, String encoding) {
        super(inputUrl, encoding);
    }

    public SparkTextFileSource(String inputUrl) {
        super(inputUrl);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        outputs[0].acceptRdd(sparkExecutor.sc.textFile(this.getInputUrl()));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTextFileSource(this.getInputUrl(), this.getEncoding());
    }
}
