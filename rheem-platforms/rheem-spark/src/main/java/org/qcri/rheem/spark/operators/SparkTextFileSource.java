package org.qcri.rheem.spark.operators;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.slf4j.LoggerFactory;

import java.util.*;

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
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        output.accept(sparkExecutor.sc.textFile(this.getInputUrl()), sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTextFileSource(this.getInputUrl(), this.getEncoding());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final OptionalLong optionalFileSize = FileSystems.getFileSize(this.getInputUrl());
        if (!optionalFileSize.isPresent()) {
            LoggerFactory.getLogger(this.getClass()).warn("Could not determine file size for {}.", this.getInputUrl());
        }

        final NestableLoadProfileEstimator mainEstimator;
        if (optionalFileSize.isPresent()) {
            mainEstimator = new NestableLoadProfileEstimator(
                    new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 500 * outputCards[0] + 5000000000L),
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
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }
}
