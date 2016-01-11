package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSet;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.plugin.Activator;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaCollectionSource extends CollectionSource implements JavaExecutionOperator {

    public JavaCollectionSource(Collection<?> collection, DataSet type) {
        super(collection, type);
    }

    @Override
    public Platform getPlatform() {
        return Activator.PLATFORM;
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        return new Stream[] { this.collection.stream() };
    }

}
