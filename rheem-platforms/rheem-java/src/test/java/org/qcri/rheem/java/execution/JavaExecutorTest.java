package org.qcri.rheem.java.execution;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.operators.JavaCollectionSource;
import org.qcri.rheem.java.operators.JavaDoWhileOperator;
import org.qcri.rheem.java.operators.JavaLocalCallbackSink;
import org.qcri.rheem.java.operators.JavaMapOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Test suite for the {@link JavaExecutor}.
 */
public class JavaExecutorTest {

    @Test
    public void testLazyExecutionResourceHandling() {
        // The JavaExecutor should not dispose resources that are consumed by lazily executed ExecutionOperators until
        // execution.
        JavaCollectionSource<Integer> source1 = new JavaCollectionSource<>(
                Collections.singleton(1),
                DataSetType.createDefault(Integer.class)
        );
        source1.setName("source1");

        JavaCollectionSource<Integer> source2 = new JavaCollectionSource<>(
                RheemArrays.asList(2, 3, 4),
                DataSetType.createDefault(Integer.class)
        );
        source2.setName("source2");

        JavaDoWhileOperator<Integer, Integer> loop = new JavaDoWhileOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                vals -> vals.stream().allMatch(v -> v > 5),
                5
        );
        loop.setName("loop");

        JavaMapOperator<Integer, Integer> increment = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<Integer, Integer>() {

                            private int increment;

                            @Override
                            public Integer apply(Integer integer) {
                                return integer + this.increment;
                            }

                            @Override
                            public void open(ExecutionContext ctx) {
                                this.increment = RheemCollections.getSingle(ctx.getBroadcast("inc"));
                            }
                        },
                        Integer.class, Integer.class
                )
        );
        increment.setName("increment");

        JavaMapOperator<Integer, Integer> id1 = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        v -> v,
                        Integer.class, Integer.class
                )
        );
        id1.setName("id1");

        JavaMapOperator<Integer, Integer> id2 = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        v -> v,
                        Integer.class, Integer.class
                )
        );
        id2.setName("id2");

        Collection<Integer> collector = new LinkedList<>();
        JavaLocalCallbackSink<Integer> sink = new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class));
        sink.setName("sink");

        loop.initialize(source2, 0);
        loop.beginIteration(increment, 0);
        source1.broadcastTo(0, increment, "inc");
        increment.connectTo(0, id1, 0);
        increment.connectTo(0, id2, 0);
        loop.endIteration(id1, 0, id2, 0);
        loop.outputConnectTo(sink, 0);

        final RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        rheemContext.execute(new RheemPlan(sink));

        Assert.assertEquals(RheemArrays.asList(6, 7, 8), collector);
    }

}
