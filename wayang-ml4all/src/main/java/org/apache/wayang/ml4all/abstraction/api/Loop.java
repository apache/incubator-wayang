/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.abstraction.api;

import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;

public abstract class Loop<R, V> extends LogicalOperator {

    /* Prepare the convergence dataset that will be used for the termination predicate
     * eg., the difference of the L2-norm of the new weights @input and the old weights which are in the context
     */
    public abstract R prepareConvergenceDataset(V input, ML4allModel context);

    /* Given the output of the convergence dataset decide if you want to continue or not */
    public abstract boolean terminate(R input);

}
