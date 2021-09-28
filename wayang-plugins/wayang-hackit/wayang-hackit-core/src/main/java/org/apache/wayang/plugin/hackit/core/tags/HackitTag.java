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
package org.apache.wayang.plugin.hackit.core.tags;

import org.apache.wayang.plugin.hackit.core.action.ActionGroup;
import org.apache.wayang.plugin.hackit.core.tagger.TaggerFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HackitTag extends from {@link ActionGroup} because is the element that allows to create
 * an element that contains the flags for the {@link org.apache.wayang.plugin.hackit.core.tagger.HackitTagger}.
 * Then, {@link org.apache.wayang.plugin.hackit.core.sniffer.HackitSniffer} can perform the right logic
 * to {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}s that have the tag
 *
 * HachitTag implements {@link Serializable} because the element could be sent out.
 */
public abstract class HackitTag implements Serializable, ActionGroup {

    /**
     * TODO: add comment, validate if apply for the use case
     */
    private List<TaggerFunction<?>> callbacks;

    /**
     * TODO: add comment, validate if apply for the use case
     */
    private Map<String, Object> callback_results;

    /**
     * Default Constructor
     */
    protected HackitTag(){
        //TODO: validate is is not better to go by delay option
        this.callbacks = new ArrayList<>();
        this.callback_results = new HashMap<>();
    }

    /**
     * TODO: add comment, validate if apply for the use case
     */
    public void callback(){
        if(hasCallback()) {
            this.callbacks.stream().forEach(
                    fun -> this.addValue(fun.getName(), fun.execute())
            );
        }
    }

    /**
     * TODO: add comment, validate if apply for the use case
     *
     * @param name
     * @param value
     */
    public void addValue(String name, Object value){
        this.callback_results.put(name, value);
    }

    /**
     * TODO: add comment, validate if apply for the use case
     *
     * @return
     */
    public abstract HackitTag getInstance();

    @Override
    public boolean equals(Object obj) {
        return this.getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
