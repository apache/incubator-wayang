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
package org.apache.wayang.plugin.hackit.core.sniffer.sniff;

import java.io.Serializable;

/**
 * Sniff is the component that evaluate if some {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple} need to
 * be treated on some way
 *
 * @param <I> type of the element that will be evaluated
 */
public interface Sniff<I> extends Serializable {

    /**
     * sniff evaluate if the <code>input</code> need to be treated on some way or contains some {@link org.apache.wayang.plugin.hackit.core.tags.HackitTag}
     * to enable the sniff
     *
     * @param input element to evaluate if is sniffable
     * @return True if the <code>input</code> need to be sniffed, False in other cases
     */
    boolean sniff(I input);
}
