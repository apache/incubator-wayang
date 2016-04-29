package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaTextFileSource extends TextFileSource implements JavaExecutionOperator {

    public JavaTextFileSource(String inputUrl) {
        super(inputUrl);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String url;
        try {
            url = new URL(this.getInputUrl()).toString();
        } catch (MalformedURLException e) {
            throw new RuntimeException("Could not parse input URL.", e);
        }

        FileSystem fs = FileSystems.getFileSystem(url).orElseThrow(
                () -> new RheemException(String.format("Cannot access file system of %s.", url))
        );

        try {
            final InputStream inputStream = fs.open(url);
            Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
            ((StreamChannel.Instance) outputs[0]).accept(lines);
        } catch (IOException e) {
            throw new RheemException(String.format("Reading %s failed.", url), e);
        }

    }


    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final OptionalLong optionalFileSize = FileSystems.getFileSize(this.getInputUrl());
        if (!optionalFileSize.isPresent()) {
            LoggerFactory.getLogger(JavaTextFileSource.class).warn("Could not determine file size for {}.", this.getInputUrl());
        }
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(0, 1, .99d, (inputCards, outputCards) -> 425 * outputCards[0] + 1400000),
                optionalFileSize.isPresent() ?
                        new DefaultLoadEstimator(0, 1, 1d, (inputCards, outputCards) -> optionalFileSize.getAsLong()) :
                        new DefaultLoadEstimator(0, 1, .5d, (inputCards, outputCards) -> outputCards[0] * 100L)
        );
        return Optional.of(mainEstimator);
    }

    @Override
    public JavaTextFileSource copy() {
        return new JavaTextFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
