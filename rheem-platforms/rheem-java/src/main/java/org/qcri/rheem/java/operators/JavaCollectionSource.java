package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaCollectionSource extends CollectionSource implements JavaExecutionOperator {

    public JavaCollectionSource(Collection<?> collection, DataSetType type) {
        super(collection, type);
    }

    @Override
    public Platform getPlatform() {
        return JavaPlatform.getInstance();
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        return new Stream[] { this.collection.stream() };
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaCollectionSource(getCollection(), getType());
    }
}
