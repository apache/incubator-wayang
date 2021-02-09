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

import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This {@link OptimizationContext} implementation aggregates several {@link OptimizationContext}s and exposes
 * their {@link OptimizationContext.OperatorContext} in an aggregated manner.
 */
public class AggregateOptimizationContext extends OptimizationContext {

    /**
     * Caches aggregated {@link OptimizationContext.OperatorContext}s.
     */
    private final Map<Operator, OperatorContext> operatorContextCache = new HashMap<>();

    /**
     * The {@link OptimizationContext}s aggregated by this instance.
     */
    private final List<OptimizationContext> optimizationContexts;

    public AggregateOptimizationContext(LoopContext loopContext) {
        super(loopContext.getOptimizationContext().getJob(),
                null,
                loopContext,
                -1,
                loopContext.getOptimizationContext().getChannelConversionGraph(),
                loopContext.getOptimizationContext().getPruningStrategies());
        this.optimizationContexts = loopContext.getIterationContexts();
    }

    @Override
    public OperatorContext addOneTimeOperator(Operator operator) {
        throw new UnsupportedOperationException("Method not supported.");
    }

    @Override
    public void addOneTimeLoop(OperatorContext operatorContext) {
        throw new UnsupportedOperationException("Method not supported.");
    }

    @Override
    public OperatorContext getOperatorContext(Operator operator) {
        return this.operatorContextCache.computeIfAbsent(operator, this::aggregateOperatorContext);
    }

    /**
     * This instance caches for efficiency reasons the aggregated {@link OperatorContext}s. This method
     * causes a re-calculation of this cache.
     */
    public void updateOperatorContexts() {
        this.operatorContextCache.values().forEach(this::updateOperatorContext);
    }

    /**
     * Aggregates the {@link OptimizationContext.OperatorContext} in the {@link #optimizationContexts} for the
     * given {@link Operator}.
     *
     * @param operator for that the aggregate {@link OptimizationContext.OperatorContext} should be created
     * @return the aggregated {@link OptimizationContext.OperatorContext} or {@code null} if nothing could be aggregated
     */
    private OperatorContext aggregateOperatorContext(Operator operator) {
        OperatorContext aggregateOperatorContext = new OperatorContext(operator);
        this.updateOperatorContext(aggregateOperatorContext);
        return aggregateOperatorContext;
    }

    private void updateOperatorContext(OperatorContext operatorContext) {
        operatorContext.resetEstimates();
        operatorContext.setNumExecutions(0);
        for (OptimizationContext partialOptimizationContext : this.optimizationContexts) {
            final OperatorContext partialOperatorContext =
                    partialOptimizationContext.getOperatorContext(operatorContext.getOperator());
            if (partialOperatorContext == null) continue;
            operatorContext.increaseBy(partialOperatorContext);
        }
    }

    @Override
    public LoopContext getNestedLoopContext(LoopSubplan loopSubplan) {
        assert this.optimizationContexts.stream().allMatch(opCtx -> opCtx.getNestedLoopContext(loopSubplan) == null);
        return null;
    }

    @Override
    public void clearMarks() {
        this.optimizationContexts.forEach(OptimizationContext::clearMarks);
    }

    @Override
    public Map<Operator, OperatorContext> getLocalOperatorContexts() {
        return Collections.emptyMap();
    }

    @Override
    public boolean isTimeEstimatesComplete() {
        return this.optimizationContexts.stream().allMatch(OptimizationContext::isTimeEstimatesComplete);
    }

    @Override
    public void mergeToBase() {
        assert this.getBase() == null : "Bases not supported.";
    }

    @Override
    public List<DefaultOptimizationContext> getDefaultOptimizationContexts() {
        return this.optimizationContexts.stream()
                .flatMap(optCtx -> optCtx.getDefaultOptimizationContexts().stream())
                .collect(Collectors.toList());
    }
}
