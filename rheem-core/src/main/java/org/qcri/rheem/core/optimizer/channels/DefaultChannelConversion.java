package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * todo
 */
public class DefaultChannelConversion extends ChannelConversion {

    private final BiFunction<Channel, Configuration, ExecutionOperator> executionOperatorFactory;

    public DefaultChannelConversion(
            ChannelDescriptor sourceChannelDescriptor,
            ChannelDescriptor targetChannelDescriptor,
            Supplier<ExecutionOperator> executionOperatorFactory) {
        this(
                sourceChannelDescriptor,
                targetChannelDescriptor,
                (sourceChannel, configuration) -> executionOperatorFactory.get()
        );
    }

    public DefaultChannelConversion(
            ChannelDescriptor sourceChannelDescriptor,
            ChannelDescriptor targetChannelDescriptor,
            BiFunction<Channel, Configuration, ExecutionOperator> executionOperatorFactory) {
        super(sourceChannelDescriptor, targetChannelDescriptor);
        this.executionOperatorFactory = executionOperatorFactory;
    }

    @Override
    public Channel convert(Channel sourceChannel, Configuration configuration) {
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(sourceChannel, configuration);
        assert executionOperator.getNumInputs() <= 1 && executionOperator.getNumOutputs() <= 1;

        final ExecutionTask task = new ExecutionTask(executionOperator, 1, 1);
        sourceChannel.addConsumer(task, 0);
        final Channel outputChannel = task.initializeOutputChannel(0, configuration);
        sourceChannel.addSibling(outputChannel);
        return outputChannel;
    }

    @Override
    public TimeEstimate estimateConversionTime(CardinalityEstimate cardinality, Configuration configuration) {
        // Create OperatorContext.
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(null, configuration);
        final OptimizationContext optimizationContext = new OptimizationContext(configuration);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(executionOperator);

        // Initialize cardinality.
        this.setCardinality(operatorContext, cardinality);

        // Estimate time.
        operatorContext.updateTimeEstimate();
        return operatorContext.getTimeEstimate();
    }

    private void setCardinality(OptimizationContext.OperatorContext operatorContext, CardinalityEstimate cardinality) {
        final int numInputs = operatorContext.getOperator().getNumInputs();
        for (int inputIndex = 0; inputIndex < numInputs; inputIndex++) {
            operatorContext.setInputCardinality(inputIndex, cardinality);
        }
        final int numOutputs = operatorContext.getOperator().getNumOutputs();
        for (int outputIndex = 0; outputIndex < numOutputs; outputIndex++) {
            operatorContext.setOutputCardinality(outputIndex, cardinality);
        }
    }
}
