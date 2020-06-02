package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.plan.rheemplan.Subplan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Mapping} translates a {@link RepeatOperator} into a {@link Subplan} with the {@link LoopOperator}.
 */
@SuppressWarnings("unused")
public class RepeatMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(this.createTransformation());
    }

    private PlanTransformation createTransformation() {
        return new PlanTransformation(
                this.createPattern(),
                this.createReplacementFactory()
        );
    }

    private SubplanPattern createPattern() {
        return SubplanPattern.createSingleton(new OperatorPattern<>(
                "repeat",
                new RepeatOperator<>(1, DataSetType.none()),
                false
        ));
    }

    private ReplacementSubplanFactory createReplacementFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<>(this::createLoopOperatorSubplan);
    }

    private Subplan createLoopOperatorSubplan(RepeatOperator<?> repeatOperator, int epoch) {
        final int numIterations = repeatOperator.getNumIterations();

        // Create
        final CollectionSource<Integer> incSource = CollectionSource.singleton(0, Integer.class);
        incSource.setName(String.format("%s (init)", repeatOperator.getName()));

        final LoopOperator<?, ?> loopOperator = new LoopOperator<>(
                repeatOperator.getType().unchecked(),
                DataSetType.createDefault(Integer.class),
                ints -> RheemCollections.getSingle(ints) >= numIterations,
                numIterations
        );
        loopOperator.setName(repeatOperator.getName());

        final MapOperator<Integer, Integer> increment = new MapOperator<>(
                i -> i + 1, Integer.class, Integer.class
        );
        increment.setName(String.format("%s (inc)", repeatOperator.getName()));

        incSource.connectTo(0, loopOperator, LoopOperator.INITIAL_CONVERGENCE_INPUT_INDEX);
        loopOperator.connectTo(LoopOperator.ITERATION_CONVERGENCE_OUTPUT_INDEX, increment, 0);
        increment.connectTo(0, loopOperator, LoopOperator.ITERATION_CONVERGENCE_INPUT_INDEX);

        // Carefully align with the slots of RepeatOperator.
        // TODO: This unfortunately does not work because we are introducing a non-source Subplan with a Source.
        return Subplan.wrap(
                Arrays.asList(
                        loopOperator.getInput(LoopOperator.INITIAL_INPUT_INDEX),
                        loopOperator.getInput(LoopOperator.ITERATION_INPUT_INDEX)
                ),
                Arrays.asList(
                        loopOperator.getOutput(LoopOperator.ITERATION_OUTPUT_INDEX),
                        loopOperator.getOutput(LoopOperator.FINAL_OUTPUT_INDEX)
                ),
                null
        );
    }
}
