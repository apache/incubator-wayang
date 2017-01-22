package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Implementation fo the {@link TextFileSink} for the {@link JavaPlatform}.
 */
public class JavaTextFileSink<T> extends TextFileSink<T> implements JavaExecutionOperator {

    public JavaTextFileSink(String textFileUrl, TransformationDescriptor<T, String> formattingDescriptor) {
        super(textFileUrl, formattingDescriptor);
    }

    public JavaTextFileSink(TextFileSink<T> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;

        JavaChannelInstance input = (JavaChannelInstance) inputs[0];
        final FileSystem fs = FileSystems.requireFileSystem(this.textFileUrl);
        final Function<T, String> formatter = javaExecutor.getCompiler().compile(this.formattingDescriptor);


        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(this.textFileUrl)))) {
            input.<T>provideStream().forEach(
                    dataQuantum -> {
                        try {
                            writer.write(formatter.apply(dataQuantum));
                            writer.write('\n');
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
            );
        } catch (IOException e) {
            throw new RheemException("Writing failed.", e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.textfilesink.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.formattingDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException();
    }

}