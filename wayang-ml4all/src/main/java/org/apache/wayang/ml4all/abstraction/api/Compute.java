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

/**
 * Created by zoi on 22/1/15.
 */
public abstract class Compute<R, V> extends LogicalOperator {

    /**
     * Performs a computation at the data unit granularity
     *
     * @param input a data unit
     * @param context
     */
    public abstract R process(V input, ML4allModel context);

    /**
     * Aggregates the output of the process() method to use in a group by
     */
    public abstract R aggregate(R input1, R input2);


}
