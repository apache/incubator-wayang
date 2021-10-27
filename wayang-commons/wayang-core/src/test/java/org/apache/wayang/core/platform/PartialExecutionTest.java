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

package org.apache.wayang.core.platform;

import org.apache.wayang.core.util.json.WayangJsonObj;
import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.configuration.KeyValueProvider;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.test.DummyExecutionOperator;
import org.apache.wayang.core.test.DummyPlatform;
import org.apache.wayang.core.test.SerializableDummyExecutionOperator;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.WayangCollections;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suites for {@link PartialExecution}s.
 */
public class PartialExecutionTest {

    @Test
    public void testJsonSerialization() {
        // Create first OperatorContext with non-serializable ExecutionOperator.
        final OptimizationContext.OperatorContext operatorContext1 = mock(OptimizationContext.OperatorContext.class);
        when(operatorContext1.getOperator()).thenReturn(new DummyExecutionOperator(1, 1, false));
        when(operatorContext1.getInputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext1.getOutputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext1.getNumExecutions()).thenReturn(1);

        // Create first OperatorContext with non-serializable ExecutionOperator.
        final OptimizationContext.OperatorContext operatorContext2 = mock(OptimizationContext.OperatorContext.class);
        when(operatorContext2.getOperator()).thenReturn(new SerializableDummyExecutionOperator(52));
        when(operatorContext2.getInputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext2.getOutputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext2.getNumExecutions()).thenReturn(1);

        Configuration configuration = new Configuration();
        final KeyValueProvider<ExecutionOperator, LoadProfileEstimator> estimatorProvider = configuration.getOperatorLoadProfileEstimatorProvider();
        ExecutionLineageNode executionLineageNode1 = new ExecutionLineageNode(operatorContext1)
                .add(estimatorProvider.provideFor((ExecutionOperator) operatorContext1.getOperator()));
        ExecutionLineageNode executionLineageNode2 = new ExecutionLineageNode(operatorContext1)
                .add(estimatorProvider.provideFor((ExecutionOperator) operatorContext2.getOperator()));
        PartialExecution original = new PartialExecution(12345L, 12, 13,
                Arrays.asList(executionLineageNode1, executionLineageNode2),
                configuration
        );
        original.addInitializedPlatform(DummyPlatform.getInstance());

        final PartialExecution.Serializer serializer = new PartialExecution.Serializer(configuration);
        final WayangJsonObj wayangJsonObj = JsonSerializables.serialize(original, false, serializer);
        final PartialExecution loaded = JsonSerializables.deserialize(wayangJsonObj, serializer, PartialExecution.class);

        Assert.assertEquals(original.getMeasuredExecutionTime(), loaded.getMeasuredExecutionTime());
        Assert.assertEquals(2, loaded.getAtomicExecutionGroups().size());
        Assert.assertEquals(1, loaded.getInitializedPlatforms().size());
        Assert.assertSame(DummyPlatform.getInstance(), WayangCollections.getAny(loaded.getInitializedPlatforms()));
    }

}
