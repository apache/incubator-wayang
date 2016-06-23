package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Default implementation of the {@link ChannelConversion}. Can be used without further subclassing.
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
    public Channel convert(Channel sourceChannel,
                           Configuration configuration,
                           Collection<OptimizationContext> optimizationContexts,
                           CardinalityEstimate optCardinality) {
        // Create the ExecutionOperator.
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(sourceChannel, configuration);
        assert executionOperator.getNumInputs() <= 1 && executionOperator.getNumOutputs() <= 1;

        // Set up the Channels and the ExecutionTask.
        final ExecutionTask task = new ExecutionTask(executionOperator, 1, 1);
        sourceChannel.addConsumer(task, 0);
        final Channel outputChannel = task.initializeOutputChannel(0, configuration);
        sourceChannel.addSibling(outputChannel);

        // Enrich the optimizationContexts.
        for (OptimizationContext optimizationContext : optimizationContexts) {
            final CardinalityEstimate cardinality = optCardinality == null ?
                    this.determineCardinality(sourceChannel, optimizationContext) :
                    optCardinality;
            this.setCardinalityAndTimeEstimate(task, optimizationContext, cardinality);
        }

        return outputChannel;
    }

    /**
     * Try to extract the {@link CardinalityEstimate} for a {@link Channel} within an {@link OptimizationContext}.
     *
     * @param channel             whose {@link CardinalityEstimate} is sought
     * @param optimizationContext provides {@link CardinalityEstimate}s
     * @return the {@link CardinalityEstimate}
     */
    private CardinalityEstimate determineCardinality(Channel channel, OptimizationContext optimizationContext) {
        final ExecutionOperator sourceOperator = channel.getProducerOperator();
        final OptimizationContext.OperatorContext sourceOpCtx = optimizationContext.getOperatorContext(sourceOperator);
        assert sourceOpCtx != null : String.format("No OperatorContext found for %s.", sourceOperator);

        final OutputSlot<?> producerSlot = channel.getProducerSlot();
        if (producerSlot != null) {
            return sourceOpCtx.getOutputCardinality(producerSlot.getIndex());
        } else if (sourceOperator.getNumInputs() == 1) {
            return sourceOpCtx.getInputCardinality(0);
        }
        throw new IllegalStateException(String.format("Could not determine cardinality of %s.", channel));
    }

    /**
     * Sets the {@link CardinalityEstimate}s for a conversion {@link ExecutionTask} in a given {@link OptimizationContext}.
     *
     * @param conversionTask      the conversion {@link ExecutionTask} that should have at most one input and one output
     * @param optimizationContext stores the {@link CardinalityEstimate}
     * @param cardinality         the {@link CardinalityEstimate}
     */
    private void setCardinalityAndTimeEstimate(ExecutionTask conversionTask,
                                OptimizationContext optimizationContext,
                                CardinalityEstimate cardinality) {
        final ExecutionOperator operator = conversionTask.getOperator();
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(operator);

        if (operator.getNumInputs() > 0) {
            assert operator.getNumInputs() == 1;
            operatorContext.setInputCardinality(0, cardinality);
        }

        if (operator.getNumOutputs() > 0) {
            assert operator.getNumOutputs() == 1;
            operatorContext.setOutputCardinality(0, cardinality);
        }

        operatorContext.updateTimeEstimate();
    }

    @Override
    public TimeEstimate estimateConversionTime(CardinalityEstimate cardinality, int numExecutions, OptimizationContext optimizationContext) {
        // Create OperatorContext.
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(null, optimizationContext.getConfiguration());
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(executionOperator);

        // Initialize cardinality and number of executions.
        operatorContext.setNumExecutions(numExecutions);
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
