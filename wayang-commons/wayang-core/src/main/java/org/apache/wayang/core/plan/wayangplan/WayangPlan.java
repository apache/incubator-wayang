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

package org.apache.wayang.core.plan.wayangplan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.optimizer.SanityChecker;
import org.apache.wayang.core.util.WayangCollections;

/**
 * A Wayang plan consists of a set of {@link Operator}s.
 */
public class WayangPlan {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Collection<Operator> sinks = new LinkedList<>();

    private boolean isLoopsIsolated = false;

    private boolean isPruned = false;


    /**
     * Prepares this {@link WayangPlan} for the optimization process if not already done.
     */
    public void prepare() {
        this.prune();
        LoopIsolator.isolateLoops(this);
    }

    /**
     * Creates a new instance and does some preprocessing (such as loop isolation).
     *
     * @param sinks the sinks of the new instance
     */
    public WayangPlan(Operator... sinks) {
        for (Operator sink : sinks) {
            this.addSink(sink);
        }
    }

    /**
     * @deprecated Use {@link WayangPlan#WayangPlan(Operator...)}.
     */
    @Deprecated
    public void addSink(Operator sink) {
        if (this.isLoopsIsolated || this.isPruned) {
            throw new IllegalStateException("Too late to add more sinks.");
        }
        Validate.isTrue(sink.isSink(), "%s is not a sink.", sink);
        Validate.isTrue(sink.getParent() == null, "%s is nested.", sink);
        this.sinks.add(sink);
    }

    /**
     * Replaces an {@code oldSink} with a {@code newSink}. As with all modifications to this instance,
     * it is up to the caller to ensure that this does not destroy the integrity of this instance.
     */
    public void replaceSink(Operator oldSink, Operator newSink) {
        if (oldSink == newSink) return;
        assert newSink.isSink();
        assert oldSink.isSink();
        if (this.sinks.remove(oldSink)) {
            assert newSink.getParent() == null;
            this.sinks.add(newSink);
        }
    }

    public Collection<Operator> getSinks() {
        return this.sinks;
    }

    /**
     * Find the source {@link Operator}s that are reachable from the sinks.
     *
     * @return the reachable sources, only top-level operators are considered
     * @see #getSinks()
     */
    public Collection<Operator> collectReachableTopLevelSources() {
        return PlanTraversal.upstream().traverse(this.sinks).getTraversedNodesWith(Operator::isSource);
    }

    /**
     * Find the {@link Operator}s in this instance by their name.
     *
     * @param name the name of the {@link Operator}s
     * @return the matching {@link Operator}s; only top-level (non-nested) ones are considered
     */
    public Collection<Operator> collectTopLevelOperatorsByName(String name) {
        return PlanTraversal.upstream().traverse(this.sinks).getTraversedNodesWith(
                operator -> name.equals(operator.getName())
        );
    }

    /**
     * Find the {@link Operator} in this instance by their name.
     *
     * @param name the name of the {@link Operator}s
     * @return the matching {@link Operator} or {@code null} if none
     * @throws WayangException if there is more than one such {@link Operator}
     */
    public Operator collectTopLevelOperatorByName(String name) throws WayangException{
        return WayangCollections.getSingleOrNull(this.collectTopLevelOperatorsByName(name));
    }

    /**
     * Prunes {@link Operator}s that do not (indirectly) contribute to a sink.
     */
    public void prune() {
        if (this.isPruned) return;

        final Set<Operator> reachableOperators = new HashSet<>();
        PlanTraversal.upstream()
                .withCallback((operator, input, output) -> {
                    reachableOperators.add(operator);
                    if (!operator.isElementary()) {
                        this.logger.warn("Not yet considering nested operators during Wayang plan pruning.");
                    }
                })
                .traverse(this.sinks);

        PlanTraversal.upstream()
                .withCallback(operator -> this.pruneUnreachableSuccessors(operator, reachableOperators))
                .traverse(this.sinks);

        this.isPruned = true;
    }

    /**
     * Prune any successor {@link Operator}s of the {@code baseOperator} that are not reachable/
     */
    private void pruneUnreachableSuccessors(Operator baseOperator, Set<Operator> reachableOperators) {
        for (int outputIndex = 0; outputIndex < baseOperator.getNumOutputs(); outputIndex++) {
            final OutputSlot<?> output = baseOperator.getOutput(outputIndex);
            new ArrayList<>(output.getOccupiedSlots()).stream()
                    .filter(occupiedInput -> !reachableOperators.contains(occupiedInput.getOwner()))
                    .forEach(occupiedInput -> {
                        this.logger.warn("Pruning unreachable {} from Wayang plan.", occupiedInput.getOwner());
                        output.unchecked().disconnectFrom(occupiedInput.unchecked());
                    });
        }

    }

    /**
     * Apply all available transformations in the {@code configuration} to this instance.
     */
    public void applyTransformations(Collection<PlanTransformation> transformations) {
        boolean isAnyChange;
        int epoch = Operator.FIRST_EPOCH;
        do {
            epoch++;
            final int numTransformations = this.applyAndCountTransformations(transformations, epoch);
            this.logger.debug("Applied {} transformations in epoch {}.", numTransformations, epoch);
            isAnyChange = numTransformations > 0;
        } while (isAnyChange);
    }

    /**
     * Apply all {@code transformations} to the {@code plan}.
     *
     * @param transformations transformations to apply
     * @param epoch           the new epoch
     * @return the number of applied transformations
     */
    private int applyAndCountTransformations(Collection<PlanTransformation> transformations, int epoch) {
        return transformations.stream()
                .mapToInt(transformation -> transformation.transform(this, epoch))
                .sum();
    }

    /**
     * Check that the given {@link WayangPlan} is as we expect it to be in the following steps.
     */
    public boolean isSane() {
        // We make some assumptions on the hyperplan. Make sure that they hold. After all, the transformations might
        // have bugs.
        return new SanityChecker(this).checkAllCriteria();
    }

    /**
     * Tells whether potential loops within this instance have been isolated.
     */
    public boolean isLoopsIsolated() {
        return this.isLoopsIsolated;
    }

    /**
     * Tells that potential loops within this instance have been isolated.
     */
    public void setLoopsIsolated() {
        this.isLoopsIsolated = true;
    }
}
