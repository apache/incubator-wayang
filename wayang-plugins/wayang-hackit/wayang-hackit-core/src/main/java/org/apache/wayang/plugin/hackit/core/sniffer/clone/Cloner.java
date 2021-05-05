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
package org.apache.wayang.plugin.hackit.core.sniffer.clone;

import java.io.Serializable;

/**
 * Cloner is the template for the functionality that take care about the clone phase
 *
 * @param <I> type of the element that it will get cloned
 * @param <O> type of the element after cloning it, it possible that could be different
 */
public interface Cloner<I, O> extends Serializable {

    /**
     * get <code>input</code> and create a clone, the output cloud be different
     * to the original, this doesn't change the behavior it just a to reduce overhead
     * in some case that is not need to have the same kind.
     *
     * @param input element to get cloned
     * @return Clone of the <code>input</code> element
     */
    O clone(I input);
}
