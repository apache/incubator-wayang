package org.qcri.rheem.tests;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.operators.*;

import java.net.URISyntaxException;
import java.util.Collection;

public class JavaExecutionPlansIT {

    private JavaPlatform jp;
    private ChannelInitializer ci;
    private ExecutionStage executionStage;


    @Before
    public void setup() {
        jp = JavaPlatform.getInstance();
        PlatformExecution platformExecution = new PlatformExecution(jp);
        ci = jp.getChannelManager().getChannelInitializer(StreamChannel.class);
        executionStage = platformExecution.createStage();
    }

    @Test
    public void testReadWrite() {

        JavaTextFileSource textFileSource = new JavaTextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());
        JavaStdoutSink<String> stdoutSink = new JavaStdoutSink<>(DataSetType.createDefault(String.class));
        textFileSource.connectTo(0, stdoutSink, 0);

        ExecutionTask t1 = new ExecutionTask(textFileSource);
        ExecutionTask t2 = new ExecutionTask(stdoutSink);
        executionStage.addTask(t1);
        executionStage.markAsStartTast(t1);
        executionStage.addTask(t2);
        executionStage.markAsTerminalTask(t2);

        Channel c1 = ci.setUpOutput(t1, 0);
        ci.setUpInput(c1, t2, 0);

        Executor executor = jp.getExecutorFactory().create();
        executor.execute(executionStage);
    }

    @Test
    public void diverseScenario2() throws URISyntaxException {
        // Build a Rheem plan.
        JavaTextFileSource textFileSource1 = new JavaTextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());
        JavaTextFileSource textFileSource2 = new JavaTextFileSource(RheemPlans.FILE_OTHER_LINES_TXT.toString());
        JavaFilterOperator<String> noCommaOperator = new JavaFilterOperator<>(
                DataSetType.createDefault(String.class),
                s -> !s.contains(","));
        JavaMapOperator<String, String> upperCaseOperator = new JavaMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        JavaUnionAllOperator<String> unionOperator = new JavaUnionAllOperator(DataSetType.createDefault(String.class));
        JavaDistinctOperator<String> distinctLinesOperator = new JavaDistinctOperator(DataSetType.createDefault(String.class));
        JavaSortOperator<String> sortOperator = new JavaSortOperator<>(DataSetType.createDefault(String.class));
        JavaStdoutSink<String> stdoutSink = new JavaStdoutSink<>(DataSetType.createDefault(String.class));

        // Read from file 1, remove commas, union with file 2, sort, upper case, then remove duplicates and output.
        textFileSource1.connectTo(0, noCommaOperator, 0);
        textFileSource2.connectTo(0, unionOperator, 0);
        noCommaOperator.connectTo(0, unionOperator, 1);
        unionOperator.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, distinctLinesOperator, 0);
        distinctLinesOperator.connectTo(0, stdoutSink, 0);


        // build stage
        ExecutionTask t1 = new ExecutionTask(textFileSource1);
        executionStage.addTask(t1);
        ExecutionTask t2 = new ExecutionTask(textFileSource2);
        executionStage.addTask(t2);
        ExecutionTask t3 = new ExecutionTask(noCommaOperator);
        executionStage.addTask(t3);
        ExecutionTask t4 = new ExecutionTask(upperCaseOperator);
        executionStage.addTask(t4);
        ExecutionTask t5 = new ExecutionTask(unionOperator);
        executionStage.addTask(t5);
        ExecutionTask t6 = new ExecutionTask(distinctLinesOperator);
        executionStage.addTask(t6);
        ExecutionTask t7 = new ExecutionTask(sortOperator);
        executionStage.addTask(t7);
        ExecutionTask t8 = new ExecutionTask(stdoutSink);
        executionStage.addTask(t8);

        // Add start and terminal tasks
        executionStage.markAsStartTast(t1);
        executionStage.markAsStartTast(t2);
        executionStage.markAsTerminalTask(t8);

        // Set up channels
        Channel c1_3 = ci.setUpOutput(t1, 0);
        ci.setUpInput(c1_3, t3, 0);
        Channel c2_5 = ci.setUpOutput(t2, 0);
        ci.setUpInput(c2_5, t5, 0);
        Channel c3_5 = ci.setUpOutput(t3, 0);
        ci.setUpInput(c3_5, t5, 1);
        Channel c4_6 = ci.setUpOutput(t4, 0);
        ci.setUpInput(c4_6, t6, 0);
        Channel c5_7 = ci.setUpOutput(t5, 0);
        ci.setUpInput(c5_7, t7, 0);
        Channel c6_8 = ci.setUpOutput(t6, 0);
        ci.setUpInput(c6_8, t8, 0);
        Channel c7_4 = ci.setUpOutput(t7, 0);
        ci.setUpInput(c7_4, t4, 0);

        // Execute
        Executor executor = jp.getExecutorFactory().create();
        executor.execute(executionStage);
    }

    /**
     * Simple counter loop .
     */
    @Test
    public void diverseScenario4() throws URISyntaxException {
        // Build a Rheem plan.
        JavaTextFileSource textFileSource1 = new JavaTextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());
        JavaMapOperator<Integer, Integer> counter = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        RheemPlans::increment,
                        DataUnitType.createBasic(Integer.class),
                        DataUnitType.createBasic(Integer.class)));
        JavaMapOperator<String, String> concat = new JavaMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        RheemPlans::concat9,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));

        JavaStdoutSink<String> stdoutSink = new JavaStdoutSink<>(DataSetType.createDefault(String.class));

        JavaLoopOperator<String, Integer> loopOperator = new JavaLoopOperator<>(DataSetType.createDefault(String.class),
                DataSetType.createDefault(Integer.class),
                new PredicateDescriptor.SerializablePredicate<Collection<Integer>>() {
                    @Override
                    public boolean test(Collection<Integer> collection) {
                        if (collection.iterator().next()>=10){
                            return true;
                        }
                        else {
                            return false;
                        }
                    }
                });

        // concat 10 times then output
        loopOperator.initialize(textFileSource1);
        loopOperator.beginIteration(concat, counter);
        loopOperator.endIteration(concat, counter);
        loopOperator.outputConnectTo(stdoutSink);

        // build stage
        ExecutionTask t1 = new ExecutionTask(textFileSource1);
        executionStage.addTask(t1);
        ExecutionTask t2 = new ExecutionTask(counter);
        executionStage.addTask(t2);
        ExecutionTask t3 = new ExecutionTask(concat);
        executionStage.addTask(t3);
        ExecutionTask t4 = new ExecutionTask(stdoutSink);
        executionStage.addTask(t4);
        ExecutionTask t5 = new ExecutionTask(loopOperator);
        executionStage.addTask(t5);

        // Add start and terminal tasks
        executionStage.markAsStartTast(t1);
        executionStage.markAsTerminalTask(t4);

        // Set up channels
        Channel c1_5 = ci.setUpOutput(t1, 0);
        ci.setUpInput(c1_5, t5, 0);
        Channel c2_5 = ci.setUpOutput(t2, 0);
        ci.setUpInput(c2_5, t5, 1);
        Channel c3_5 = ci.setUpOutput(t3, 0);
        ci.setUpInput(c2_5, t5, 2);
        Channel c5_3 = ci.setUpOutput(t5, 0);
        ci.setUpInput(c5_3, t3, 0);
        Channel c5_2 = ci.setUpOutput(t5, 1);
        ci.setUpInput(c5_2, t2, 0);
        Channel c5_4 = ci.setUpOutput(t5, 2);
        ci.setUpInput(c5_4, t4, 0);

        // Execute
        Executor executor = jp.getExecutorFactory().create();
        executor.execute(executionStage);

    }

}

