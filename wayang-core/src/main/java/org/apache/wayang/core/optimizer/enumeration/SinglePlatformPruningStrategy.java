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

package org.apache.wayang.core.optimizer.enumeration;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.platform.Platform;

/**
 * This {@link PlanEnumerationPruningStrategy} retains only the best {@link PlanImplementation}s employing a single
 * {@link Platform} only.
 * <p>There is one caveat, though: If for some reason the most efficient way to communicate for two
 * {@link org.apache.wayang.core.plan.wayangplan.ExecutionOperator}s from the same {@link Platform} goes over another
 * {@link Platform}, then we will prune the corresponding {@link PlanImplementation}. The more complete way is to
 * look only for non-cross-platform {@link org.apache.wayang.core.platform.Junction}s. We neglect this issue for now.</p>
 */
@SuppressWarnings("unused")
public class SinglePlatformPruningStrategy implements PlanEnumerationPruningStrategy {


    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        planEnumeration.getPlanImplementations().removeIf(
                planImplementation -> planImplementation.getUtilizedPlatforms().size() > 1
        );
    }

}
