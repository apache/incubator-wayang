package io.rheem.rheem.java.operators;

import io.rheem.rheem.basic.operators.TextFileSink;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.function.TransformationDescriptor;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.optimizer.costs.LoadProfileEstimator;
import io.rheem.rheem.core.optimizer.costs.LoadProfileEstimators;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.core.util.fs.FileSystem;
import io.rheem.rheem.core.util.fs.FileSystems;
import io.rheem.rheem.java.channels.CollectionChannel;
import io.rheem.rheem.java.channels.JavaChannelInstance;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.java.execution.JavaExecutor;
import io.rheem.rheem.java.platform.JavaPlatform;

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
