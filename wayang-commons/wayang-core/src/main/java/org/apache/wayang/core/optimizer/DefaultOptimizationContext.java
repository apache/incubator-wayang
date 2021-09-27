/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.optimizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.channels.ChannelConversionGraph;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerationPruningStrategy;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.Subplan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.WayangArrays;

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
     * Create a new instance and adds all {@link Operator}s in the {@link WayangPlan}.
     *
     * @param job the optimization task; loops should already be isolated
     */
    public static DefaultOptimizationContext createFrom(Job job) {
        DefaultOptimizationContext instance = new DefaultOptimizationContext(job);
        PlanTraversal.upstream()
                .withCallback(instance::addOneTimeOperator)
                .traverse(job.getWayangPlan().getSinks());
        return instance;
    }

    /**
     * Create a new instance.
     *
     * @param job the optimization task; loops should already be isolated
     */
    public DefaultOptimizationContext(Job job) {
        super(job);
    }

    /**
     * Forks an {@link DefaultOptimizationContext} by providing a write-layer on top of the {@code base}.
     */
    public DefaultOptimizationContext(OptimizationContext base) {
        super(base.getJob(),
                base,
                base.hostLoopContext,
                base.getIterationNumber(),
                base.getChannelConversionGraph(),
                base.getPruningStrategies());
    }

    /**
     * Creates a new instance. Useful for testing.
     *
     * @param job      whose optimization thew new instance backs
     * @param operator the single {@link Operator} of this instance
     */
    public DefaultOptimizationContext(Job job, Operator operator) {
        super(job);
        this.addOneTimeOperator(operator);
    }

    /**
     * Creates a new (nested) instance for the given {@code loop}.
     */
    DefaultOptimizationContext(LoopSubplan loop, LoopContext hostLoopContext, int iterationNumber) {
        super(hostLoopContext.getOptimizationContext().getJob(),
                null,
                hostLoopContext,
                iterationNumber,
                hostLoopContext.getOptimizationContext().getChannelConversionGraph(),
                hostLoopContext.getOptimizationContext().getPruningStrategies());
        this.addOneTimeOperators(loop);
    }

    /**
     * Base constructor.
     */
    private DefaultOptimizationContext(Job job,
                                       OptimizationContext base,
                                       LoopContext hostLoopContext,
                                       int iterationNumber,
                                       ChannelConversionGraph channelConversionGraph,
                                       List<PlanEnumerationPruningStrategy> pruningStrategies) {
        super(job, base, hostLoopContext, iterationNumber, channelConversionGraph, pruningStrategies);
    }

    @Override
    public OperatorContext addOneTimeOperator(Operator operator) {
        final OperatorContext operatorContext = new OperatorContext(operator);
        this.operatorContexts.putIfAbsent(operator, operatorContext);
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
                    && operatorContext.timeEstimate == null
                    && WayangArrays.anyMatch(operatorContext.getOutputCardinalities(), Objects::nonNull)) {
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
        for (Map.Entry<Operator, OptimizationContext.OperatorContext> entry : this.operatorContexts.entrySet()) {
            this.getBase().operatorContexts.merge(
                    entry.getKey(),
                    entry.getValue(),
                    OperatorContext::merge
            );
        }
    }

    @Override
    public List<DefaultOptimizationContext> getDefaultOptimizationContexts() {
        return Collections.singletonList(this);
    }

    /**
     * Create a shallow copy of this instance.
     *
     * @return the shallow copy
     */
    public DefaultOptimizationContext copy() {
        final DefaultOptimizationContext copy = new DefaultOptimizationContext(
                this.getJob(),
                this.getBase(),
                this.getLoopContext(),
                this.getIterationNumber(),
                this.getChannelConversionGraph(),
                this.getPruningStrategies()
        );

        // Make copies of the OperatorContexts.
        for (Operator operator : operatorContexts.keySet()) {
            copy.addOneTimeOperator(operator);
        }
        // Now merge the original to the copied OperatorContexts.
        // Note: This must be a separate step! Each operation above potentially creates multiple OperatorContexts.
        for (Map.Entry<Operator, OperatorContext> entry : copy.operatorContexts.entrySet()) {
            Operator operator = entry.getKey();
            OperatorContext opCtxCopy = entry.getValue();
            OperatorContext originalOpCtx = this.operatorContexts.get(operator);
            if (originalOpCtx != null) opCtxCopy.merge(originalOpCtx);
        }

        // Loops are not supported yet.
        assert this.loopContexts.isEmpty();

        return copy;
    }

}
