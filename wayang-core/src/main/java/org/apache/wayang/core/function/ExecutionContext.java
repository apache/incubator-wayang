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

package org.apache.wayang.core.function;

import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.platform.Platform;

import java.util.Collection;

/**
 * While a function is executed on a certain {@link Platform}, allows access to some information of the context in
 * which the function is being executed.
 */
public interface ExecutionContext {

    /**
     * Accesses a broadcast.
     *
     * @param name name of the broadcast
     * @param <T>  type of the broadcast
     * @return the broadcast
     */
    <T> Collection<T> getBroadcast(String name);

    /**
     * If this instance reflects the state of execution inside of a {@link LoopSubplan}, then retrieve the
     * number of the current iteration.
     *
     * @return the iteration number, start at {@code 0}, or {@code -1} if there is no surrounding {@link LoopSubplan}
     */
    int getCurrentIteration();

}
