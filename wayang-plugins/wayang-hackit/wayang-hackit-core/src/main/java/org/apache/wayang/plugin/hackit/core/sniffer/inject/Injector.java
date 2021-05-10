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
package org.apache.wayang.plugin.hackit.core.sniffer.inject;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Injector is the component on the Sniffer that it get looking to get a {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 * form outside and added on the current process
 *
 * @param <T> type of the tuple that need to be process
 */
public interface Injector<T> extends Serializable {

    /**
     *
     *
     * @param element
     * @param iterator
     * @return
     */
    Iterator<T> inject(T element, Iterator<T> iterator);

    /**
     * Evaluate if the <code>element</code> need to skipped or process
     *
     * @param element that is evaluated
     * @return True is need to be process, False in other cases
     */
    boolean is_skip_element(T element);

    /**
     * Evaluate if the <code>element</code> need to halt the job or not
     *
     * @param element that is evaluated
     * @return True if the process need to be halt, False in other cases
     */
    boolean is_halt_job(T element);
}
