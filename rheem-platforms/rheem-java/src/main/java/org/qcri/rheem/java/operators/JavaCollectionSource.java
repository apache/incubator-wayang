package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Collection;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaCollectionSource extends CollectionSource implements JavaExecutionOperator {

    public JavaCollectionSource(Collection<?> collection, DataSetType type) {
        super(collection, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == 0;
        assert outputs.length == 1;
        outputs[0].acceptStream(this.getCollection().stream());
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaCollectionSource(this.getCollection(), this.getType());
    }
}
