package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
        OptimizationContext aggregateContext = this.loopContext.getAggregateContext();
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
            final PlanImplementation planImplementation = new PlanImplementation(
                    this.loopEnumeration,
                    new HashMap<>(1),
                    this.loopContext.getOptimizationContext()
            );
            planImplementation.addLoopImplementation(loop, loopImplementation);
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
        for (Iterator<PlanImplementation> iterator = curBodyEnumeration.getPlanImplementations().iterator(); iterator.hasNext(); ) {
            PlanImplementation loopImpl = iterator.next();
            for (InputSlot<?> loopBodyInput : loopHead.getLoopBodyInputs()) {
                final OutputSlot<?> occupant = loopBodyInput.getOccupant();
                if (occupant == null) continue;
                if (!this.addFeedbackConnection(loopImpl, occupant, loopBodyInput, optimizationContext)) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Adds feedback {@link Junction}s for a {@link PlanImplementation}.
     *
     * @param loopImpl      to which the {@link Junction} should be added
     * @param occupant      {@link OutputSlot} (abstract) that provides feedback connections
     * @param loopBodyInput {@link InputSlot} (abstract) which takes the feedback connection
     * @return whether the adding the feedback connection was successful
     */
    private boolean addFeedbackConnection(PlanImplementation loopImpl,
                                          OutputSlot<?> occupant,
                                          InputSlot<?> loopBodyInput,
                                          OptimizationContext optimizationContext) {
        final List<InputSlot<?>> execLoopBodyInputs = new ArrayList<>(loopImpl.findExecutionOperatorInputs(loopBodyInput));
        final Collection<OutputSlot<?>> execOutputs = loopImpl.findExecutionOperatorOutput(occupant);
        for (OutputSlot<?> execOutput : execOutputs) {
            final Junction existingJunction = loopImpl.getJunction(execOutput);
            if (existingJunction != null) {
                LoggerFactory.getLogger(this.getClass()).debug(
                        "Need to override existing {} for {} while closing loop.",
                        existingJunction, execOutput
                );
                execLoopBodyInputs.addAll(existingJunction.getTargetInputs());
            }
            final Junction junction = optimizationContext.getChannelConversionGraph().findMinimumCostJunction(
                    execOutput, execLoopBodyInputs, optimizationContext, false
            );
            if (junction == null) return false;

            loopImpl.putJunction(execOutput, junction);
        }
        return true;
    }

}
