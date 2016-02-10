package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.plugin.JavaPlatform;

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
    public Platform getPlatform() {
        return JavaPlatform.getInstance();
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        URL url;
        try {
            url = new URL(this.getInputUrl());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Could not parse input URL.", e);
        }
        if ("file".equals(url.getProtocol())) {
            final Path path = new File(url.getPath()).toPath();
            try {
                return new Stream[]{Files.lines(path)};
            } catch (IOException e) {
                throw new RuntimeException(e);
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
