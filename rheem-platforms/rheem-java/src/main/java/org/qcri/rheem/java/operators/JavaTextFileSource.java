package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaTextFileSource extends TextFileSource implements JavaExecutionOperator {

    public JavaTextFileSource(String inputUrl) {
        super(inputUrl);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        URL url;
        try {
            url = new URL(this.getInputUrl());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Could not parse input URL.", e);
        }
        if ("file".equals(url.getProtocol())) {
            final Path path = new File(url.getPath()).toPath();
            try {
                outputs[0].acceptStream(Files.lines(path));
            } catch (IOException e) {
                throw new RheemException("Reading failed.", e);
            }
        } else {
            throw new RuntimeException(String.format("Unsupported URL: %s", url));
        }

    }

    @Override
    public JavaTextFileSource copy() {
        return new JavaTextFileSource(this.getInputUrl());
    }
}
