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

/**
 * This interface provides a reference-counting scheme, e.g., to manage allocated external resources. The initial
 * number of references after the object instantiation is always {@code 0}.
 */
public interface ReferenceCountable {

    /**
     * Tells the number of references on this instance.
     *
     * @return the number of references
     */
    int getNumReferences();

    /**
     * Declare that there is a new reference obtained on this instance.
     */
    void noteObtainedReference();

    /**
     * Declare that a reference on this instance has been discarded. Optionally, dispose this instance if there are
     * no remaining references.
     *
     * @param isDisposeIfUnreferenced whether to dispose this instance if there are no more references
     */
    void noteDiscardedReference(boolean isDisposeIfUnreferenced);

    /**
     * Dispose this instance if there are no more references to it.
     *
     * @return whether this instance is not referenced any more
     */
    boolean disposeIfUnreferenced();

    /**
     * <i>Optional operation.</i> Tell whether this instance has been disposed.
     *
     * @return whether this instance has been disposed
     */
    default boolean isDisposed() {
        throw new UnsupportedOperationException(String.format("%s does not support this method.", this.getClass()));
    }
}
