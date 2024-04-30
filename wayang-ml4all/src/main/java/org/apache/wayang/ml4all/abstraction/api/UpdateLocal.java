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

public abstract class UpdateLocal<R, V> extends LogicalOperator {

    /**
     * Computes the new value of the global variable
     *
     * @param input the ouput of the aggregate of the {@link Compute}
     * @param context
     */
    public abstract R process(V input, ML4allModel context);

    /**
     * Assigns the new value of the global variable to the {@link ML4allModel}
     * @param input the output of the process method
     * @param context
     * @return the new {@link ML4allModel}
     */
    public abstract ML4allModel assign (R input, ML4allModel context); //TODO: deprecated class -> put input in a singleton list
}
