package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
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
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == 1;
        assert outputs.length == 0;

        StreamChannel.Instance input = (StreamChannel.Instance) inputs[0];
        final FileSystem fs = FileSystems.requireFileSystem(this.textFileUrl);
        final Function<T, String> formatter = compiler.compile(this.formattingDescriptor);


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
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExecutedEagerly() {
        return true;
    }
}
