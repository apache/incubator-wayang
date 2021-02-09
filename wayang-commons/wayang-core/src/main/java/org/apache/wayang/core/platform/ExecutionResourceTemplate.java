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

package org.apache.wayang.core.platform;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.util.AbstractReferenceCountable;

/**
 * Implements various functionalities of an {@link ExecutionResource}.
 */
public abstract class ExecutionResourceTemplate extends AbstractReferenceCountable implements ExecutionResource {

    /**
     * Maintains this instance.
     */
    private final CompositeExecutionResource container;

    /**
     * Creates a new instance and registers it with an {@link CompositeExecutionResource}. If the latter is an
     * {@link ExecutionResource} itself, then this instance obtains a reference on it during its life cycle.
     *
     * @param container that maintains this instance or {@code null} if none
     */
    protected ExecutionResourceTemplate(CompositeExecutionResource container) {
        this.container = container;
        if (this.container != null) {
            this.container.register(this);
            this.container.noteObtainedReference();
        }
    }

    @Override
    protected void disposeUnreferenced() {
        this.dispose();
    }

    @Override
    public void dispose() throws WayangException {
        try {
            this.doDispose();
        } catch (Throwable t) {
            throw new WayangException(String.format("Releasing %s failed.", this), t);
        } finally {
            if (this.container != null) {
                this.container.unregister(this);
                this.container.noteDiscardedReference(true);
            }
        }
    }

    /**
     * Performs the actual disposing work of this instance.
     *
     * @throws Throwable in case anything goes wrong
     */
    abstract protected void doDispose() throws Throwable;

}
