package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.List;

/**
 * Implementation of the {@link TextFileSink} operator for the Spark platform.
 */
public class SparkTextFileSink<T> extends TextFileSink<T> implements SparkExecutionOperator {


    public SparkTextFileSink(String textFileUrl, TransformationDescriptor<T, String> formattingDescriptor) {
        super(textFileUrl, formattingDescriptor);
    }

    public SparkTextFileSink(TextFileSink<T> that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == 1;
        assert outputs.length == 0;
        JavaRDD<T> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();
        final Function<T, String> formattingFunction = compiler.compile(this.formattingDescriptor, this, inputs);
        inputRdd.map(formattingFunction).saveAsTextFile(this.textFileUrl);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }
}
