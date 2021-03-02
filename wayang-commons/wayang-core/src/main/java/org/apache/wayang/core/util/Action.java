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

package org.apache.wayang.core.util;

import org.apache.logging.log4j.LogManager;

/**
 * This interface represents any piece of code that takes no input and produces no output but may fail.
 */
@FunctionalInterface
public interface Action {

    /**
     * Perform this action.
     *
     * @throws Throwable in case anything goes wrong
     */
    void execute() throws Throwable;

    /**
     * Performs this actionl. If any error occurs, it will be merely logged.
     */
    default void executeSafe() {
        try {
            this.execute();
        } catch (Throwable t) {
            LogManager.getLogger(Action.class).error("Execution failed.", t);
        }
    }
}

