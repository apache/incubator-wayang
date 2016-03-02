package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link LoopOperator}.
 */
public class JavaLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     */
    public JavaLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        super(inputType, convergenceType, criterionPredicate);
    }

    public JavaLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor) {
        super(inputType, convergenceType, criterionDescriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Predicate<Collection<ConvergenceType>> stoppingCondition = compiler.compile(this.criterionDescriptor);
        Boolean endloop = false;

        Collection<ConvergenceType> convergenceCollection = new ArrayList<>();
        Stream<InputType> input = null;
        switch (this.getState()){
            case NOT_STARTED:
                input = inputs[0].provideStream();
                convergenceCollection.add(null);
                break;
            case RUNNING:
                if (inputs[1].canProvideCollection()) {
                    convergenceCollection = inputs[1].provideCollection();
                }
                else {
                    convergenceCollection = (Collection<ConvergenceType>) inputs[1].provideStream().collect(Collectors.toList());
                }
                endloop = stoppingCondition.test(convergenceCollection);
                input = inputs[2].provideStream();
                break;
            case FINISHED:
                return;

        }

        if (endloop) {
            // final loop output
            outputs[2].acceptStream(input);
            this.setState(State.FINISHED);
        }
        else {
            outputs[0].acceptStream(input);
            outputs[1].acceptCollection(convergenceCollection);
            this.setState(State.RUNNING);

        }

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaLoopOperator<>(this.getInputType(), this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation());
    }
}
