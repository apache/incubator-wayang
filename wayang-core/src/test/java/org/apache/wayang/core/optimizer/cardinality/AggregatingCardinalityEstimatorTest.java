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

package org.apache.wayang.core.optimizer.cardinality;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link AggregatingCardinalityEstimator}.
 */
public class AggregatingCardinalityEstimatorTest {

    @Test
    public void testEstimate() {
        OptimizationContext optimizationContext = mock(OptimizationContext.class);
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());

        CardinalityEstimator partialEstimator1 = new DefaultCardinalityEstimator(0.9, 1, false, cards -> cards[0] * 2);
        CardinalityEstimator partialEstimator2 = new DefaultCardinalityEstimator(0.8, 1, false, cards -> cards[0] * 3);
        CardinalityEstimator estimator = new AggregatingCardinalityEstimator(
                Arrays.asList(partialEstimator1, partialEstimator2)
        );

        CardinalityEstimate inputEstimate = new CardinalityEstimate(10, 100, 0.3);
        CardinalityEstimate outputEstimate = estimator.estimate(optimizationContext, inputEstimate);
        CardinalityEstimate expectedEstimate = new CardinalityEstimate(2 * 10, 2 * 100, 0.3 * 0.9);

        Assert.assertEquals(expectedEstimate, outputEstimate);
    }


}
