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
import org.apache.wayang.plugin.hackit.core.tagger.wrapper.template.FunctionTemplate;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

public class FunctionWrapperHackit <IDType, I, O> extends HackitTagger implements FunctionTemplate<HackitTuple<IDType, I>, HackitTuple<IDType, O>> {

    private FunctionTemplate<I, O> function;

    public FunctionWrapperHackit(FunctionTemplate<I, O> function) {
        this.function = function;
    }

    @Override
    public HackitTuple<IDType, O> execute(HackitTuple<IDType, I> v1) {
        this.preTaggingTuple(v1);
        O result = this.function.execute(v1.getValue());
        return this.postTaggingTuple(v1, result);
    }
}
