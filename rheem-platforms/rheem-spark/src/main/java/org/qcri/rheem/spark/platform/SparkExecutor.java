package org.qcri.rheem.spark.platform;

import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.platform.Executor;

import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;


public class SparkExecutor implements Executor{

    public static final Executor.Factory FACTORY = () -> new SparkExecutor();

    public FunctionCompiler compiler = new FunctionCompiler();


    @Override
    public void evaluate(ExecutionOperator executionOperator) {
        if (!executionOperator.isSink()) {
            throw new IllegalArgumentException("Cannot evaluate execution operator: it is not a sink");
        }

        if (!(executionOperator instanceof SparkExecutionOperator)) {
            throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                    "Execution plan contains non-Java operator %s.", executionOperator));
        }

        evaluate0((SparkExecutionOperator) executionOperator);
    }

    private JavaRDDLike[] evaluate0(SparkExecutionOperator operator) {
        // Resolve all the input streams for this operator.
        JavaRDDLike[] inputStreams = new JavaRDDLike[operator.getNumInputs()];
        for (int i = 0; i < inputStreams.length; i++) {
            final OutputSlot outputSlot = operator.getInput(i).getOccupant();
            if (outputSlot == null) {
                throw new IllegalStateException("Cannot evaluate execution operator: There is an unsatisfied input.");
            }

            final Operator inputOperator = outputSlot.getOwner();
            if (!(inputOperator instanceof SparkExecutionOperator)) {
                throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                        "Execution plan contains non-Spark operator %s.", inputOperator));
            }

            JavaRDDLike[] outputStreams = evaluate0((SparkExecutionOperator) inputOperator);
            int outputSlotIndex = 0;
            for (; outputSlot != inputOperator.getOutput(outputSlotIndex); outputSlotIndex++) ;
            inputStreams[i] = outputStreams[outputSlotIndex];
        }

        return operator.evaluate(inputStreams, this.compiler);
    }
}
