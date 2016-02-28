package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

/**
 * Spark implementation of the {@link LoopOperator}.
 */
public class SparkLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     */
    public SparkLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        super(inputType, convergenceType, criterionPredicate);
    }

    public SparkLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor) {
        super(inputType, convergenceType, criterionDescriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler,
                         SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Predicate<Collection<ConvergenceType>> stoppingCondition = this.criterionDescriptor.getJavaImplementation();
        Boolean endloop = false;

        Collection<ConvergenceType> convergenceCollection = null;
        JavaRDD<InputType> input = null;
        switch (this.getState()){
            case NOT_STARTED:
                input = inputs[0].provideRdd();
                convergenceCollection = Collections.emptyList();
                break;
            case RUNNING:
                convergenceCollection = (Collection<ConvergenceType>) inputs[1].provideRdd().cache().collect();
                endloop = stoppingCondition.test(convergenceCollection);
                input = inputs[2].provideRdd();
                break;
            case FINISHED:
                return;

        }

        if (endloop) {
            // final loop output
            outputs[2].acceptRdd(input);
            this.setState(State.FINISHED);
        }
        else {
            outputs[0].acceptRdd(input);
            outputs[1].acceptRdd(inputs[1].provideRdd());
            this.setState(State.RUNNING);

        }

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkLoopOperator<>(this.getInputType(), this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation());
    }
}
