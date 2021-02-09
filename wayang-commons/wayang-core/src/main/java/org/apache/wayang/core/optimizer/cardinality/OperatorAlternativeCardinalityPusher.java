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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.util.Tuple;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link OperatorAlternative}s.
 */
public class OperatorAlternativeCardinalityPusher extends AbstractAlternativeCardinalityPusher {

    /**
     * Maintains a {@link CardinalityEstimationTraversal} for each {@link OperatorAlternative.Alternative}.
     */
    private final List<Tuple<OperatorAlternative.Alternative, CardinalityEstimationTraversal>> alternativeTraversals;

    public OperatorAlternativeCardinalityPusher(final OperatorAlternative operatorAlternative,
                                                final Configuration configuration
    ) {
        super(operatorAlternative);
        this.alternativeTraversals = operatorAlternative.getAlternatives().stream()
                .map(alternative -> {
                    final CardinalityEstimationTraversal traversal =
                            CardinalityEstimationTraversal.createPushTraversal(alternative, configuration);
                    return new Tuple<>(alternative, traversal);
                })
                .collect(Collectors.toList());
    }

    @Override
    public void pushThroughAlternatives(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        final OptimizationContext optimizationContext = opCtx.getOptimizationContext();
        for (Tuple<OperatorAlternative.Alternative, CardinalityEstimationTraversal> alternativeTraversal :
                this.alternativeTraversals) {
            this.pushThroughPath(alternativeTraversal, configuration, optimizationContext);
        }
    }

    /**
     * Trigger the {@link CardinalityEstimationTraversal} for the given {@code traversal}.
     */
    private void pushThroughPath(Tuple<OperatorAlternative.Alternative, CardinalityEstimationTraversal> traversal,
                                 Configuration configuration,
                                 OptimizationContext optimizationCtx) {
        // Perform the push.
        traversal.field1.traverse(optimizationCtx, configuration);
    }

}
