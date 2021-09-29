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
package org.apache.wayang.plugin.hackit.core.tagger.wrapper;

import org.apache.wayang.plugin.hackit.core.tagger.HackitTagger;
import org.apache.wayang.plugin.hackit.core.tagger.wrapper.template.FunctionTemplateSystem;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.function.Predicate;

/**
 * PredicateWrapperHackit is an implementation of {@link HackitTagger} where Hackit manages the logic
 * before and after of tagging process. Additionally, it performs unwrap of the tuple to be handled by the
 * original function. This original function is a {@link Predicate} function and therefore returns a {@link Boolean}
 *
 * @param <IDType> Type of {@link org.apache.wayang.plugin.hackit.core.tuple.header.Header} key of the {@link HackitTuple}
 * @param <I> Input Type of the original Tuple to be evaluated
 */
public class PredicateWrapperHackit<IDType, I>
        extends HackitTagger
        implements FunctionTemplateSystem<HackitTuple<IDType, I>, Boolean> {

    /**
     * Original predicate that will evaluate the data to give a True or False value
     */
    private FunctionTemplateSystem<I, Boolean> function;

    /**
     * Default Construct
     *
     * @param function is the predicate that will be Wrapped by the {@link PredicateWrapperHackit}
     */
    public PredicateWrapperHackit(FunctionTemplateSystem<I, Boolean> function) {
        this.function = function;
    }

    @Override
    public Boolean execute(HackitTuple<IDType, I> input) {
        this.preTaggingTuple(input);
        Boolean result = this.function.execute(input.getValue());
        this.postTaggingTuple(input);
        return result;
    }
}
