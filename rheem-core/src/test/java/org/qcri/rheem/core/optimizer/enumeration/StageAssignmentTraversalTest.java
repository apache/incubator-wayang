package org.qcri.rheem.core.optimizer.enumeration;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.executionplan.test.TestChannel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.test.MockFactory;

import java.util.Collections;

/**
 * Test suite for {@link StageAssignmentTraversal}.
 */
public class StageAssignmentTraversalTest {

    @Test
    public void testCircularPlatformAssignment() {
        final Platform mockedPlatformA = MockFactory.createPlatform("A");
        final Platform mockedPlatformB = MockFactory.createPlatform("B");
        final Platform mockedPlatformC = MockFactory.createPlatform("C");

        //           /----------------------------\
        // source A <                              join A -> sink A
        //           \-> map B -> map C -> map A -/

        // Build up ExecutionTasks.
        final ExecutionOperator sourceOpA = MockFactory.createExecutionOperator("source A", 0, 1, mockedPlatformA);
        final ExecutionTask sourceTaskA = new ExecutionTask(sourceOpA);

        final ExecutionOperator joinOpA = MockFactory.createExecutionOperator("join A", 2, 1, mockedPlatformA);
        final ExecutionTask joinTaskA = new ExecutionTask(joinOpA);

        final ExecutionOperator mapOpB = MockFactory.createExecutionOperator("map B", 1, 1, mockedPlatformB);
        final ExecutionTask mapTaskB = new ExecutionTask(mapOpB);

        final ExecutionOperator mapOpC = MockFactory.createExecutionOperator("map C", 1, 1, mockedPlatformC);
        final ExecutionTask mapTaskC = new ExecutionTask(mapOpC);

        final ExecutionOperator mapOpA = MockFactory.createExecutionOperator("map A", 1, 1, mockedPlatformA);
        final ExecutionTask mapTaskA = new ExecutionTask(mapOpA);

        final ExecutionOperator sinkOpA = MockFactory.createExecutionOperator("sink A", 1, 0, mockedPlatformA);
        final ExecutionTask sinkTaskA = new ExecutionTask(sinkOpA);

        // Connect them using Channels.
        Channel sourceTaskAChannel1 = new TestChannel(true);
        sourceTaskA.setOutputChannel(0, sourceTaskAChannel1);
        sourceTaskAChannel1.addConsumer(joinTaskA, 0);
        sourceTaskAChannel1.addConsumer(mapTaskB, 0);

        Channel mapTaskBChannel1 = new TestChannel(false);
        mapTaskB.setOutputChannel(0, mapTaskBChannel1);
        mapTaskBChannel1.addConsumer(mapTaskC, 0);

        Channel mapTaskCChannel1 = new TestChannel(false);
        mapTaskC.setOutputChannel(0, mapTaskCChannel1);
        mapTaskCChannel1.addConsumer(mapTaskA, 0);

        Channel mapTaskAChannel1 = new TestChannel(false);
        mapTaskA.setOutputChannel(0, mapTaskAChannel1);
        mapTaskAChannel1.addConsumer(joinTaskA, 1);

        Channel joinTaskAChannel1 = new TestChannel(false);
        joinTaskA.setOutputChannel(0, joinTaskAChannel1);
        joinTaskAChannel1.addConsumer(sinkTaskA, 0);

        // Assign platforms.
        ExecutionTaskFlow executionTaskFlow = new ExecutionTaskFlow(Collections.singleton(sinkTaskA));
        final ExecutionPlan executionPlan = StageAssignmentTraversal.assignStages(executionTaskFlow);
        Assert.assertEquals(1, executionPlan.getStartingStages().size());

        ExecutionStage stage1 = executionPlan.getStartingStages().stream().findAny().get();
        Assert.assertEquals(1, stage1.getStartTasks().size());

        ExecutionTask stage1Task1 = stage1.getStartTasks().stream().findAny().get();
        Assert.assertEquals(sourceTaskA, stage1Task1);

        Assert.assertEquals(2, stage1.getSuccessors().size());
    }

    @Test
    public void testZigZag() {
        final Platform mockedPlatformA = MockFactory.createPlatform("A");
        final Platform mockedPlatformB = MockFactory.createPlatform("B");

        //             /-(map B)----------------(join B)-\
        // (source A)-<           \           /           >-(sink A)
        //             \------------(join A)-------------/
        //  stage 1     stage 2      stage 3     stage 4    stage 5

        // Build up ExecutionTasks.
        final ExecutionOperator sourceOpA = MockFactory.createExecutionOperator("source A", 0, 1, mockedPlatformA);
        final ExecutionTask sourceTaskA = new ExecutionTask(sourceOpA);

        final ExecutionOperator joinOpA = MockFactory.createExecutionOperator("join A", 2, 1, mockedPlatformA);
        final ExecutionTask joinTaskA = new ExecutionTask(joinOpA);

        final ExecutionOperator sinkOpA = MockFactory.createExecutionOperator("sink A", 2, 0, mockedPlatformA);
        final ExecutionTask sinkTaskA = new ExecutionTask(sinkOpA);

        final ExecutionOperator mapOpB = MockFactory.createExecutionOperator("map B", 1, 1, mockedPlatformB);
        final ExecutionTask mapTaskB = new ExecutionTask(mapOpB);

        final ExecutionOperator joinOpB = MockFactory.createExecutionOperator("join B", 2, 1, mockedPlatformB);
        final ExecutionTask joinTaskB = new ExecutionTask(joinOpB);


        // Connect them using Channels.
        Channel sourceTaskAChannel1 = new TestChannel(true);
        sourceTaskA.setOutputChannel(0, sourceTaskAChannel1);
        sourceTaskAChannel1.addConsumer(mapTaskB, 0);
        sourceTaskAChannel1.addConsumer(joinTaskA, 0);

        Channel mapTaskBChannel = new TestChannel(true);
        mapTaskB.setOutputChannel(0, mapTaskBChannel);
        mapTaskBChannel.addConsumer(joinTaskA, 1);
        mapTaskBChannel.addConsumer(joinTaskB, 0);

        Channel joinTaskAChannel = new TestChannel(true);
        joinTaskA.setOutputChannel(0, joinTaskAChannel);
        joinTaskAChannel.addConsumer(sinkTaskA, 0);
        joinTaskAChannel.addConsumer(joinTaskB, 1);

        Channel joinTaskBChannel = new TestChannel(true);
        joinTaskB.setOutputChannel(0, joinTaskBChannel);
        joinTaskBChannel.addConsumer(sinkTaskA, 1);

        // Assign platforms.
        ExecutionTaskFlow executionTaskFlow = new ExecutionTaskFlow(Collections.singleton(sinkTaskA));
        final ExecutionPlan executionPlan = StageAssignmentTraversal.assignStages(executionTaskFlow);

        Assert.assertEquals(1, executionPlan.getStartingStages().size());

        ExecutionStage stage1 = executionPlan.getStartingStages().stream().findAny().get();
        Assert.assertEquals(1, stage1.getStartTasks().size());

        ExecutionTask stage1Task1 = stage1.getStartTasks().stream().findAny().get();
        Assert.assertEquals(sourceTaskA, stage1Task1);

        Assert.assertEquals(2, stage1.getSuccessors().size());

    }


}
