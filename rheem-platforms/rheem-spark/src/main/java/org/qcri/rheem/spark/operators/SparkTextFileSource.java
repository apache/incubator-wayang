package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
        final JavaRDD<String> rdd = sparkExecutor.sc.textFile(this.getInputUrl());
        this.name(rdd);
        output.accept(rdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTextFileSource(this.getInputUrl(), this.getEncoding());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
//        final OptionalLong optionalFileSize;
//        if (this.getInputUrl() == null) {
//            optionalFileSize = OptionalLong.empty();
//        } else {
//            optionalFileSize = FileSystems.getFileSize(this.getInputUrl());
//            if (!optionalFileSize.isPresent()) {
//                LoggerFactory.getLogger(this.getClass()).warn("Could not determine file size for {}.", this.getInputUrl());
//            }
//        }

        final String specification = configuration.getStringProperty("rheem.spark.textfilesource.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
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
