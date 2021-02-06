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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Method wrapper that ensures that it is called only once.
 */
public abstract class OneTimeExecutable {

    /**
     * Guard to avoid double execution.
     */
    private final AtomicBoolean isNotExecuted = new AtomicBoolean(true);

    /**
     * Invoke {@link #doExecute()} unless it has been executed already. Also, ensure that it will not be invoked
     * a second time.
     *
     * @return whether the method invocation resulted in invoking {@link #doExecute()}
     */
    protected boolean tryExecute() {
        if (this.isNotExecuted.getAndSet(false)) {
            this.doExecute();
            return true;
        }
        return false;
    }

    /**
     * Invoke {@link #doExecute()}. Also, ensure that it will not be invoked
     * a second time.
     *
     * @throws IllegalStateException if {@link #doExecute()} has been already invoked
     */
    protected void execute() throws IllegalStateException {
        if (!this.tryExecute()) {
            throw new IllegalStateException(String.format("%s cannot be executed a second time.", this));
        }
    }

    /**
     * Performs the actual work of this instance. Should only be invoked via {@link #execute()} and
     * {@link #tryExecute()}.
     */
    protected abstract void doExecute();

}
