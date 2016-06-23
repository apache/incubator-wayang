package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.RheemArrays;

import java.util.*;

/**
 * This implementation of {@link OptimizationContext} represents a direct mapping from {@link OptimizationContext.OperatorContext}
 * to executions of the respective {@link Operator}s.
 */
public class DefaultOptimizationContext extends OptimizationContext {

    /**
     * {@link OperatorContext}s of one-time {@link Operator}s (i.e., that are not nested in a loop).
     */
    private final Map<Operator, OperatorContext> operatorContexts = new HashMap<>();

    /**
     * {@link LoopContext}s of one-time {@link LoopSubplan}s (i.e., that are not
     * nested in a loop themselves).
     */
    private final Map<LoopSubplan, LoopContext> loopContexts = new HashMap<>();

    /**
     * Create a new, plain instance.
     */
    public DefaultOptimizationContext(Configuration configuration) {
        super(configuration);
    }

    /**
     * Forks an {@link DefaultOptimizationContext} by providing a write-layer on top of the {@code base}.
     */
    public DefaultOptimizationContext(DefaultOptimizationContext base) {
        super(base.getConfiguration(),
                base,
                base.hostLoopContext,
                base.getIterationNumber(),
                base.getChannelConversionGraph(),
                base.getPruningStrategies());
    }

    /**
     * Create a new instance.
     *
     * @param rheemPlan that the new instance should describe; loops should already be isolated
     */
    public DefaultOptimizationContext(RheemPlan rheemPlan, Configuration configuration) {
        super(configuration);
        PlanTraversal.upstream()
                .withCallback(this::addOneTimeOperator)
                .traverse(rheemPlan.getSinks());
    }

    /**
     * Creates a new instance. Useful for testing.
     *
     * @param operator the single {@link Operator} of this instance
     */
    public DefaultOptimizationContext(Operator operator, Configuration configuration) {
        super(configuration);
        this.addOneTimeOperator(operator);
    }

    /**
     * Creates a new (nested) instance for the given {@code loop}.
     */
    DefaultOptimizationContext(LoopSubplan loop, LoopContext hostLoopContext, int iterationNumber, Configuration configuration) {
        super(configuration,
                null,
                hostLoopContext,
                iterationNumber,
                hostLoopContext.getOptimizationContext().getChannelConversionGraph(),
                hostLoopContext.getOptimizationContext().getPruningStrategies());
        this.addOneTimeOperators(loop);
    }

    @Override
    public OperatorContext addOneTimeOperator(Operator operator) {
        final OperatorContext operatorContext = new OperatorContext(operator);
        this.operatorContexts.put(operator, operatorContext);
        if (!operator.isElementary()) {
            if (operator.isLoopSubplan()) {
                this.addOneTimeLoop(operatorContext);
            } else if (operator.isAlternative()) {
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                operatorAlternative.getAlternatives().forEach(this::addOneTimeOperators);
            } else {
                assert operator.isSubplan();
                this.addOneTimeOperators((Subplan) operator);
            }
        }
        return operatorContext;
    }

    /**
     * Add {@link DefaultOptimizationContext}s for the {@code loop} that is executed once within this instance.
     */
    public void addOneTimeLoop(OperatorContext operatorContext) {
        this.loopContexts.put((LoopSubplan) operatorContext.getOperator(), new LoopContext(operatorContext));
    }

    /**
     * Return the {@link OperatorContext} of the {@code operator}.
     *
     * @param operator a one-time {@link Operator} (i.e., not in a nested loop)
     * @return the {@link OperatorContext} for the {@link Operator} or {@code null} if none
     */
    public OperatorContext getOperatorContext(Operator operator) {
        OperatorContext operatorContext = this.operatorContexts.get(operator);
        if (operatorContext == null) {
            if (this.getBase() != null) {
                operatorContext = this.getBase().getOperatorContext(operator);
            } else if (this.hostLoopContext != null) {
                operatorContext = this.hostLoopContext.getOptimizationContext().getOperatorContext(operator);
            }
        }
        return operatorContext;
    }

    /**
     * Retrieve the {@link LoopContext} for the {@code loopSubplan}.
     */
    public LoopContext getNestedLoopContext(LoopSubplan loopSubplan) {
        LoopContext loopContext = this.loopContexts.get(loopSubplan);
        if (loopContext == null && this.getBase() != null) {
            loopContext = this.getBase().getNestedLoopContext(loopSubplan);
        }
        return loopContext;
    }


    /**
     * Calls {@link OperatorContext#clearMarks()} for all nested {@link OperatorContext}s.
     */
    public void clearMarks() {
        this.operatorContexts.values().forEach(OperatorContext::clearMarks);
        this.loopContexts.values().stream()
                .flatMap(loopCtx -> loopCtx.getIterationContexts().stream())
                .forEach(OptimizationContext::clearMarks);
    }

    @Override
    public Map<Operator, OperatorContext> getLocalOperatorContexts() {
        return this.operatorContexts;
    }

    @Override
    public boolean isTimeEstimatesComplete() {
        boolean isComplete = true;
        for (OperatorContext operatorContext : operatorContexts.values()) {
            if (operatorContext.getOperator().isExecutionOperator()
                    && operatorContext.getTimeEstimate() == null
                    && RheemArrays.anyMatch(operatorContext.getOutputCardinalities(), Objects::nonNull)) {
                this.logger.warn("No TimeEstimate for {}.", operatorContext);
                isComplete = false;
            }
        }

        if (this.getBase() != null) {
            isComplete &= this.getBase().isTimeEstimatesComplete();
        }

        for (LoopContext loopContext : this.loopContexts.values()) {
            for (OptimizationContext iterationContext : loopContext.getIterationContexts()) {
                isComplete &= iterationContext.isTimeEstimatesComplete();
            }
        }

        return isComplete;
    }

    @Override
    public DefaultOptimizationContext getBase() {
        return (DefaultOptimizationContext) super.getBase();
    }

    @Override
    public void mergeToBase() {
        if (this.getBase() == null) return;
        assert this.loopContexts.isEmpty() : "Merging loop contexts is not supported yet.";
        this.getBase().operatorContexts.putAll(this.operatorContexts);
    }

    @Override
    public Collection<DefaultOptimizationContext> getDefaultOptimizationContexts() {
        return Collections.singleton(this);
    }
}
