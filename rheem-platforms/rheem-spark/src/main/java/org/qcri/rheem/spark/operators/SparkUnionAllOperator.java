package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Spark implementation of the {@link UnionAllOperator}.
 */
public class SparkUnionAllOperator<Type>
        extends UnionAllOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkUnionAllOperator(DataSetType<Type> type) {
        super(type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkUnionAllOperator(UnionAllOperator<Type> that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         SparkExecutor sparkExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<Type> inputRdd0 = input0.provideRdd();
        final JavaRDD<Type> inputRdd1 = input1.provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd0.union(inputRdd1);
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkUnionAllOperator<>(this.getInputType0());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final String specification = configuration.getStringProperty("rheem.spark.union.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean isExecutedEagerly() {
        return false;
    }
}
