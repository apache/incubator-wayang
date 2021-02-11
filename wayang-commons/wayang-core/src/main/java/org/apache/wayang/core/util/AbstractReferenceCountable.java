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
import org.apache.logging.log4j.Logger;
/**
 * Implements a template for {@link ReferenceCountable} objects.
 */
public abstract class AbstractReferenceCountable implements ReferenceCountable {

    private static final Logger logger = LogManager.getLogger(ReferenceCountable.class);

    /**
     * Maintains the number of references on this object.
     */
    private int numReferences = 0;

    /**
     * Marks whether this instance has been disposed to avoid unwanted resurrection, double disposal etc.
     *
     * @see #disposeIfUnreferenced()
     */
    private boolean isDisposed = false;

    @Override
    public boolean disposeIfUnreferenced() {
        if (this.getNumReferences() == 0) {
            assert !this.isDisposed() : String.format("%s has already been disposed.", this);
            logger.debug("Discarding {} for being unreferenced.", this);
            this.disposeUnreferenced();
            this.isDisposed = true;
            return true;
        }
        return false;
    }

    /**
     * Dispose this instance, which is not referenced anymore. This method should always be invoked through
     * {@link #disposeUnreferenced()}
     */
    protected abstract void disposeUnreferenced();

    @Override
    public int getNumReferences() {
        return this.numReferences;
    }

    @Override
    public void noteObtainedReference() {
        assert !this.isDisposed() : String.format("%s should not be resurrected.", this);
        this.numReferences++;
        logger.trace("{} has {} (+1) references now.", this, this.getNumReferences());
    }

    @Override
    public void noteDiscardedReference(boolean isDisposeIfUnreferenced) {
        assert this.numReferences > 0 : String.format("Reference on %s discarded, although the reference counter is 0.", this);
        this.numReferences--;
        logger.trace("{} has {} (-1) references now.", this, this.getNumReferences());
        if (isDisposeIfUnreferenced) {
            this.disposeIfUnreferenced();
        }
    }

    @Override
    public boolean isDisposed() {
        return this.isDisposed;
    }

}
