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

import java.util.Map.Entry;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This mapping can be used to encapsulate subplans by connecting slots (usually <b>against</b> the data flow direction,
 * i.e., outer output slot to inner output slot, inner input slot to outer input slot).
 */
public class SlotMapping {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Map<Slot<?>, Slot<?>> upstreamMapping = new HashMap<>();

    private Map<Slot<?>, Collection> downstreamMapping = null;

    /**
     * Create a new instance that maps all {@link Slot}s of the given {@link Operator} to themselves.
     *
     * @param operator that will be decorated with the new instance
     * @return the new instance
     */
    public static SlotMapping createIdentityMapping(Operator operator) {
        return wrap(operator, operator);
    }

    /**
     * Creates a new instance where the {@link Slot}s of the {@code wrapper} are mapped to the {@link Slot}s of the
     * {@code wrappee}, thereby matching their {@link Slot}s by their index (as in {@link Operator#getInput(int)} and
     * {@link Operator#getOutput(int)}).
     *
     * @param wrappee the inner {@link Operator}
     * @param wrapper the outer {@link Operator}
     * @return the new instance
     */
    public static SlotMapping wrap(Operator wrappee, Operator wrapper) {
        SlotMapping slotMapping = new SlotMapping();
        slotMapping.mapAllUpsteam(wrapper.getAllOutputs(), wrappee.getAllOutputs());
        slotMapping.mapAllUpsteam(wrappee.getAllInputs(), wrapper.getAllInputs());
        return slotMapping;
    }

    public void mapAllUpsteam(InputSlot<?>[] sources, InputSlot<?>[] targets) {
        if (sources.length != targets.length) {
            throw new IllegalArgumentException(String.format("Incompatible number of input slots between %s and %s.",
                    Arrays.toString(sources), Arrays.toString(targets)));
        }
        for (int i = 0; i < sources.length; i++) {
            this.mapUpstream(sources[i], targets[i]);
        }
    }

    public void mapAllUpsteam(OutputSlot<?>[] sources, OutputSlot<?>[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            this.mapUpstream(sources[i], targets[i]);
        }
    }

    public void mapUpstream(InputSlot<?> source, InputSlot<?> target) {
        if (target == null) {
            this.upstreamMapping.remove(source);
            this.downstreamMapping = null;
            return;
        }

        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException(String.format("Incompatible slots given: %s -> %s", source, target));
        }

        this.upstreamMapping.put(source, target);
        this.downstreamMapping = null;
    }

    public void mapUpstream(OutputSlot<?> source, OutputSlot<?> target) {
        if (target == null) {
            this.upstreamMapping.remove(source);
            this.downstreamMapping = null;
            return;
        }

        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException(String.format("Incompatible slots given: %s -> %s", source, target));
        }

        this.upstreamMapping.put(source, target);
        this.downstreamMapping = null;
    }

    public <T> InputSlot<T> resolveUpstream(InputSlot<T> source) {
        if (source.getOccupant() != null) {
            this.logger.warn("Trying to resolve (upstream) an InputSlot with an occupant.");
        }
        return (InputSlot<T>) this.upstreamMapping.get(source);
    }

    public <T> OutputSlot<T> resolveUpstream(OutputSlot<T> source) {
        return (OutputSlot<T>) this.upstreamMapping.get(source);
    }

    public <T> Collection<InputSlot<T>> resolveDownstream(InputSlot<T> source) {
        return (Collection<InputSlot<T>>) this.getOrCreateDownstreamMapping().getOrDefault(source, Collections.emptyList());
    }

    public <T> Collection<OutputSlot<T>> resolveDownstream(OutputSlot<T> source) {
        if (!source.getOccupiedSlots().isEmpty()) {
            this.logger.warn("Trying to resolve (downstream) an OutputSlot with occupiers.");
        }
        return (Collection<OutputSlot<T>>) this.getOrCreateDownstreamMapping().getOrDefault(source, Collections.emptyList());
    }

    /**
     * Retrieves {@link #downstreamMapping} or creates it if it does not exist.
     *
     * @return {@link #downstreamMapping}
     */
    private Map<Slot<?>, Collection> getOrCreateDownstreamMapping() {
        if (this.downstreamMapping == null) {
            Map<Slot<?>, Collection> map = new HashMap<>();
            for (Entry<Slot<?>, Slot<?>> slotSlotEntry : this.upstreamMapping.entrySet()) {
                Object key = slotSlotEntry.getKey();
                map.computeIfAbsent(slotSlotEntry.getValue(), k -> new LinkedList<Object>())
                    .add(key);
            }
            this.downstreamMapping = map;
        }

        return this.downstreamMapping;
    }

    /**
     * Replace the mappings from an old, wrapped operator with a new wrapped operator.
     *
     * @param oldOperator the old wrapped operator
     * @param newOperator the new wrapped operator
     */
    public void replaceInputSlotMappings(Operator oldOperator, Operator newOperator) {
        if (oldOperator.getParent() == newOperator) {
            // Default strategy: The oldOperator is now wrapped by the newOperator.
            final SlotMapping oldToNewSlotMapping = oldOperator.getContainer().getSlotMapping();
            for (int i = 0; i < oldOperator.getNumInputs(); i++) {
                final InputSlot<?> oldInput = oldOperator.getInput(i);
                assert oldInput != null : String.format("No %dth input for %s (for %s).", i, oldOperator, newOperator);
                if (oldInput.getOccupant() != null) continue;

                final InputSlot<?> outerInput = this.resolveUpstream(oldInput);
                if (outerInput != null) {
                    this.delete(oldInput);
                    final InputSlot<?> newInput = oldToNewSlotMapping.resolveUpstream(oldInput);
                    if (newInput != null) this.mapUpstream(newInput, outerInput);
                }
            }

        } else {
            // Fallback strategy.
            this.logger.warn("Using bare indices to replace {} (parent {}) with {}.", oldOperator, oldOperator.getParent(), newOperator);
            assert oldOperator.getNumInputs() == newOperator.getNumInputs()
                    : String.format("Operators %s and %s are not matching.", oldOperator, newOperator);

            for (int i = 0; i < oldOperator.getNumInputs(); i++) {
                final InputSlot<?> oldInput = oldOperator.getInput(i);
                final InputSlot<?> newInput = newOperator.getInput(i);

                final InputSlot<?> outerInput = this.resolveUpstream(oldInput);
                if (outerInput != null) {
                    this.mapUpstream(newInput, outerInput);
                    this.delete(oldInput);
                }
            }
        }
    }

    /**
     * Removes an existing mapping.
     *
     * @param key the key of the mapping to remove
     */
    private void delete(InputSlot<?> key) {
        this.upstreamMapping.remove(key);
        this.downstreamMapping = null;
    }

    /**
     * Removes an existing mapping.
     *
     * @param key the key of the mapping to remove
     */
    private void delete(OutputSlot<?> key) {
        this.upstreamMapping.remove(key);
        this.downstreamMapping = null;
    }

    /**
     * Replace the mappings from an old, wrapped operator with a new wrapped operator.
     *
     * @param oldOperator the old wrapped operator
     * @param newOperator the new wrapped operator
     */
    @SuppressWarnings("unchecked")
    public void replaceOutputSlotMappings(Operator oldOperator, Operator newOperator) {
        if (oldOperator.getParent() == newOperator) {
            // Default strategy: The oldOperator is now wrapped by the newOperator.
            final Map<Slot<?>, Collection> downstreamMapping = this.getOrCreateDownstreamMapping();
            final SlotMapping oldToNewSlotMapping = oldOperator.getContainer().getSlotMapping();
            for (int i = 0; i < oldOperator.getNumOutputs(); i++) {
                final OutputSlot<?> oldOutput = oldOperator.getOutput(i);
                final Collection<OutputSlot<?>> outerOutputs = downstreamMapping.get(oldOutput);
                if (outerOutputs == null) continue;

                // We cannot have multiple OutputSlots, we could not map them downstream to a single OutputSlot.
                final OutputSlot<?> newOutput = WayangCollections.getSingleOrNull(
                        (Collection<OutputSlot<?>>) oldToNewSlotMapping
                                .getOrCreateDownstreamMapping()
                                .getOrDefault(oldOutput, Collections.emptySet())
                );

                for (OutputSlot<?> outerOutput : outerOutputs) {
                    this.mapUpstream(outerOutput, newOutput);
                }
            }

        } else {
            // Fallback strategy.
            this.logger.warn("Using bare indices to replace {} (parent {}) with {}.", oldOperator, oldOperator.getParent(), newOperator);
            assert oldOperator.getNumOutputs() == newOperator.getNumOutputs()
                    : String.format("Operators %s and %s are not matching.", oldOperator, newOperator);

            for (int i = 0; i < oldOperator.getNumOutputs(); i++) {
                final OutputSlot<?> oldOutput = oldOperator.getOutput(i);
                final OutputSlot<?> newOutput = newOperator.getOutput(i);

                this.upstreamMapping.entrySet().stream()
                        .filter(entry -> entry.getValue() == oldOutput)
                        .findFirst()
                        .map(Map.Entry::getKey)
                        .ifPresent(outerOutput -> this.mapUpstream((OutputSlot<?>) outerOutput, newOutput));
                // No need for delete as we are replacing the old mapping.
            }
        }
    }

    /**
     * Functionally compose two instances. The {@link OutputSlot}s of this instance are followed to {@link InputSlot}s
     * to the other instances, thereby applying the mapping step.
     *
     * @param that the instance to be composed to this one
     * @return a {@link Map} mapping inner {@link InputSlot}s of the second instance to the inner {@link OutputSlot}s
     * of the first one
     */
    public Map<InputSlot, OutputSlot> compose(SlotMapping that) {
        Map<InputSlot, OutputSlot> result = new HashMap<>(2);
        that.upstreamMapping.entrySet().stream()
                .filter(entry -> entry.getKey().isInputSlot())
                .forEach(entry -> {
                    InputSlot thatOuterInputSlot = (InputSlot) entry.getValue();
                    final OutputSlot allegedThisOuterOutputSlot = thatOuterInputSlot.getOccupant();
                    if (allegedThisOuterOutputSlot == null) return;
                    final OutputSlot thisInnerOutputSlot = this.resolveUpstream(allegedThisOuterOutputSlot);
                    if (thisInnerOutputSlot == null) return;
                    final InputSlot thatInnerInputSlot = (InputSlot) entry.getKey();
                    result.put(thatInnerInputSlot, thisInnerOutputSlot);
                });

        return result;
    }

    /**
     * Retrieves the upstream mapping of {@link Slot}s. Do not modify!
     *
     * @return the upstream mapping
     */
    public Map<Slot<?>, Slot<?>> getUpstreamMapping() {
        return this.upstreamMapping;
    }

}
