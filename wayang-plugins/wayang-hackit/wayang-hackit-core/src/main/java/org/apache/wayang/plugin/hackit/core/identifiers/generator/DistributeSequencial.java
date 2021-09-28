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
package org.apache.wayang.plugin.hackit.core.identifiers.generator;

import org.apache.wayang.plugin.hackit.core.identifiers.HackitIDGenerator;

/**
 * DistributeSequencial is an instance of {@link HackitIDGenerator}.
 *
 * DistributeSequencial assumes not parallel jobs running, and help in small debugs, or single node work
 */
public class DistributeSequencial extends HackitIDGenerator<Integer, Long> {

    /**
     * current save the number of elements already generated
     */
    long current = 0;

    /**
     * Adds 1 to <code>current<code/> and assign the previous number to the ID
     *
     * @return long that represent the ID, this could have repetition on parallel processing
     */
    @Override
    public Long generateId() {
        Long tmp = current;
        current++;
        return tmp;
    }
}
