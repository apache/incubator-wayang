package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.Tuple;

import java.util.HashMap;

/**
 * Enumerator for {@link LoopSubplan}s.
 */
public class LoopEnumerator extends OneTimeExecutable {

    private final OptimizationContext.LoopContext loopContext;

    private final PlanEnumerator planEnumerator;

    private PlanEnumeration loopEnumeration;

    public LoopEnumerator(PlanEnumerator planEnumerator, OptimizationContext.LoopContext loopContext) {
        this.planEnumerator = planEnumerator;
        this.loopContext = loopContext;
    }

    public PlanEnumeration enumerate() {
        this.tryExecute();
        return this.loopEnumeration;
    }

    @Override
    protected void doExecute() {
        // Create aggregate iteration contexts.
        OptimizationContext aggregateContext =
                this.loopContext.createAggregateContext(0, this.loopContext.getIterationContexts().size());
        LoopSubplan loop = this.loopContext.getLoop();

        // Create the end result.
        this.loopEnumeration = new PlanEnumeration();
        for (OutputSlot<?> loopOutput : loop.getAllOutputs()) {
            if (loopOutput.getOccupiedSlots().isEmpty()) {
                this.loopEnumeration.getServingOutputSlots().add(new Tuple<>(loopOutput, null));
            } else {
                for (InputSlot<?> input : loopOutput.getOccupiedSlots()) {
                    this.loopEnumeration.getServingOutputSlots().add(new Tuple<>(loopOutput, input));
                }
            }
        }
        for (InputSlot<?> loopInput : loop.getAllInputs()) {
            this.loopEnumeration.getRequestedInputSlots().add(loopInput);
        }

        // Enumerate the loop body (for now, only a single loop body).
        final PlanEnumerator loopBodyEnumerator =
                this.planEnumerator.forkFor(this.loopContext.getLoop().getLoopHead(), aggregateContext);
        final PlanEnumeration loopBodyEnumeration = loopBodyEnumerator.enumerate(true);

        for (PlanImplementation loopBodyImplementation : loopBodyEnumeration.getPlanImplementations()) {
            final LoopImplementation loopImplementation = new LoopImplementation(this.loopContext.getLoop());
            loopImplementation.addIterationEnumeration(
                    this.loopContext.getLoop().getNumExpectedIterations(), loopBodyImplementation
            );
            final PlanImplementation planImplementation = new PlanImplementation(this.loopEnumeration, new HashMap<>(1));
            planImplementation.getLoopImplementations().put(loop, loopImplementation);
            planImplementation.addToTimeEstimate(loopImplementation.getTimeEstimate());
            this.loopEnumeration.add(planImplementation);
        }
    }
}
