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
package org.apache.wayang.plugin.hackit.core.tagger;

/**
 * TaggerFunction is the template for the function that will be use inside of {@link HackitTagger}
 *
 * @param <T> output type of the execution
 */
public interface TaggerFunction<T> {
    /**
     * It execute the function of tagger, and generate and output
     *
     * @return result of the execution
     */
    T execute();

    /**
     * Get the name of the function, this is use a identifier
     *
     * @return {@link String} that contains the name of the {@link TaggerFunction}
     */
    String getName();

}
