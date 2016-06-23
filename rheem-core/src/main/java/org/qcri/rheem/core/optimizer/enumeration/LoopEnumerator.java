package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

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
                this.loopContext.createAggregateContext();
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

        // Enumerate feedback connections.
        this.addFeedbackConnections(loopBodyEnumeration, null, aggregateContext);

        // Wrap each PlanImplementation in a new PlanImplementation that subsumes the whole loop instead of iterations.
        for (PlanImplementation loopBodyImplementation : loopBodyEnumeration.getPlanImplementations()) {
            final LoopImplementation loopImplementation = new LoopImplementation(this.loopContext.getLoop());
            loopImplementation.addIterationEnumeration(
                    this.loopContext.getLoop().getNumExpectedIterations(), loopBodyImplementation
            );
            final PlanImplementation planImplementation = new PlanImplementation(this.loopEnumeration, new HashMap<>(1), aggregateContext);
            planImplementation.getLoopImplementations().put(loop, loopImplementation);
            this.loopEnumeration.add(planImplementation);
        }
    }

    /**
     * Adds feedback {@link Junction}s for all {@link PlanImplementation}s in the {@code curBodyEnumeration}.
     *
     * @param curBodyEnumeration       a {@link PlanEnumeration} within which the feedback {@link Junction}s should be added
     * @param successorBodyEnumeration implemetns the successor iteration
     * @param optimizationContext      used for the createion of {@code curBodyEnumeration}
     */
    private void addFeedbackConnections(PlanEnumeration curBodyEnumeration,
                                        PlanEnumeration successorBodyEnumeration,
                                        OptimizationContext optimizationContext) {
        assert successorBodyEnumeration == null : "Multiple loop enumerations not supported, yet.";

        // Find the LoopHeadOperator.
        LoopHeadOperator loopHead = this.loopContext.getLoop().getLoopHead();

        // Go through all loop body InputSlots.
        for (InputSlot<?> loopBodyInput : loopHead.getLoopBodyInputs()) {
            final OutputSlot<?> occupant = loopBodyInput.getOccupant();
            if (occupant == null) continue;
            for (PlanImplementation loopImpl : curBodyEnumeration.getPlanImplementations()) {
                this.addFeedbackConnection(loopImpl, occupant, loopBodyInput, optimizationContext);
            }
        }
    }

    /**
     * Adds feedback {@link Junction}s for a {@link PlanImplementation}.
     *
     * @param loopImpl      to which the {@link Junction} should be added
     * @param occupant      {@link OutputSlot} (abstract) that provides feedback connections
     * @param loopBodyInput {@link InputSlot} (abstract) which takes the feedback connection
     */
    private void addFeedbackConnection(PlanImplementation loopImpl,
                                       OutputSlot<?> occupant,
                                       InputSlot<?> loopBodyInput,
                                       OptimizationContext optimizationContext) {
        final List<InputSlot<?>> execLoopBodyInputs = new ArrayList<>(loopImpl.findExecutionOperatorInputs(loopBodyInput));
        final Collection<OutputSlot<?>> execOutputs = loopImpl.findExecutionOperatorOutput(occupant);
        for (OutputSlot<?> execOutput : execOutputs) {
            assert loopImpl.getJunction(execOutput) == null
                    : String.format("There already is %s for %s. Need to implement merging logic.",
                    loopImpl.getJunction(execOutput), execOutput);
            final Junction junction = optimizationContext.getChannelConversionGraph().findMinimumCostJunction(
                    execOutput, execLoopBodyInputs, optimizationContext
            );
            //Junction.create(execOutput, execLoopBodyInputs, optimizationContext);
            if (junction != null) {
                loopImpl.putJunction(execOutput, junction);
            }
        }
    }

}
