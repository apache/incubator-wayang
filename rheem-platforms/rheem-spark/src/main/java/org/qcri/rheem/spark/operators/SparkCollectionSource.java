package org.qcri.rheem.spark.operators;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Provides a {@link Collection} to a Spark job.
 */
public class SparkCollectionSource<Type> extends CollectionSource<Type> implements SparkExecutionOperator {

    public SparkCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        outputs[0].acceptRdd(sparkExecutor.sc.parallelize(this.getCollectionAsList()));
    }

    private List<Type> getCollectionAsList() {
        final Collection<Type> collection = this.getCollection();
        if (this.collection instanceof List) {
            return (List<Type>) this.collection;
        }
        return new ArrayList<>(collection);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 1500 * outputCards[0] + 2000L),
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 100 * outputCards[0] + 2000),
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 5 * outputCards[0] + 2000),
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 0),
                0.75d,
                2000
        );

        return Optional.of(mainEstimator);
    }
}
