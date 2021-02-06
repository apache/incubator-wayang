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

package org.apache.wayang.basic.mapping;

import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.basic.operators.LoopOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.RepeatOperator;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.Subplan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangCollections;

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
                ints -> WayangCollections.getSingle(ints) >= numIterations,
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
