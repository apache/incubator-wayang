package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * {@link Breakpoint} implementation that disrupts execution if all aggregated {@link Breakpoint}s request a disruption.
 * However, if no {@link Breakpoint} conjuncts are set up, then it will never break.
 */
public class ConjunctiveBreakpoint implements Breakpoint {

    private final List<Breakpoint> conjuncts;

    public ConjunctiveBreakpoint(Breakpoint... conjuncts) {
        this.conjuncts = new LinkedList<>(Arrays.asList(conjuncts));
        assert this.conjuncts.stream().allMatch(Objects::nonNull);
    }

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage,
                                      ExecutionState state,
                                      OptimizationContext optimizationContext) {
        return this.conjuncts.isEmpty() ||
                this.conjuncts.stream().anyMatch(conjunct -> conjunct.permitsExecutionOf(stage, state, optimizationContext));
    }

    public void addConjunct(Breakpoint breakpoint) {
        assert breakpoint != null;
        this.conjuncts.add(breakpoint);
    }
}
