package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Collection;
import java.util.Optional;

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
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(0, 1, 0.9d, (inCards, outCards) -> 4 * outCards[0] + 1000000),
                LoadEstimator.createFallback(0, 1)
        ));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCollectionSource(this.getCollection(), this.getType());
    }
}
