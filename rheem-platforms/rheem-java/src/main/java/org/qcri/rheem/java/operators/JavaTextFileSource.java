package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Source;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.plugin.Activator;

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

    private final OutputSlot<String> outputSlot = new OutputSlot<>("output", this, String.class);

    private final OutputSlot<String>[] outputSlots = new OutputSlot[]{this.outputSlot};

    public JavaTextFileSource(String inputUrl) {
        super(inputUrl);
    }

    @Override
    public Platform getPlatform() {
        return Activator.PLATFORM;
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams) {
        URL url;
        try {
            url = new URL(getInputUrl());
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
    public OutputSlot[] getAllOutputs() {
        return this.outputSlots;
    }

}
