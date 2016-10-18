package org.qcri.rheem.core.optimizer.channels;

import org.junit.BeforeClass;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.test.DummyExecutionOperator;
import org.qcri.rheem.core.test.DummyExternalReusableChannel;
import org.qcri.rheem.core.test.DummyNonReusableChannel;
import org.qcri.rheem.core.test.DummyReusableChannel;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Test suite for {@link ChannelConversionGraph}.
 */
public class ChannelConversionGraphTest {

    private static DefaultChannelConversion reusableToNonReusableChannelConversion;

    private static DefaultChannelConversion nonReusableToReusableChannelConversion;

    private static DefaultChannelConversion reusableToExternalChannelConversion;

    private static DefaultChannelConversion nonReusableToExternalChannelConversion;

    private static DefaultChannelConversion externalToNonReusableChannelConversion;

    private static Configuration configuration;

    private static Supplier<ExecutionOperator> createDummyExecutionOperatorFactory(ChannelDescriptor channelDescriptor) {
        return () -> {
            ExecutionOperator execOp = new DummyExecutionOperator(1, 1, false);
            execOp.getSupportedOutputChannels(0).add(channelDescriptor);
            return execOp;
        };
    }

    @BeforeClass
    public static void initializeChannelConversions() {
        reusableToNonReusableChannelConversion = new DefaultChannelConversion(
                DummyReusableChannel.DESCRIPTOR,
                DummyNonReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyNonReusableChannel.DESCRIPTOR)
        );
        nonReusableToReusableChannelConversion = new DefaultChannelConversion(
                DummyNonReusableChannel.DESCRIPTOR,
                DummyReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyReusableChannel.DESCRIPTOR)
        );
        reusableToExternalChannelConversion = new DefaultChannelConversion(
                DummyReusableChannel.DESCRIPTOR,
                DummyExternalReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyExternalReusableChannel.DESCRIPTOR)
        );
        nonReusableToExternalChannelConversion = new DefaultChannelConversion(
                DummyNonReusableChannel.DESCRIPTOR,
                DummyNonReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyNonReusableChannel.DESCRIPTOR)
        );
        externalToNonReusableChannelConversion = new DefaultChannelConversion(
                DummyExternalReusableChannel.DESCRIPTOR,
                DummyNonReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyNonReusableChannel.DESCRIPTOR)
        );
        configuration = new Configuration();
    }

    @Test
    public void findDirectConversion() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(configuration);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);
        destOperator1.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(configuration);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext
        );
    }

    @Test
    public void findIntricateConversion() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(configuration);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext
        );
    }

    @Test
    public void findIntricateConversion2() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyExternalReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(configuration);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext
        );
    }


}