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
package org.apache.wayang.plugin.hackit.core.tuple.header;

import org.apache.wayang.plugin.hackit.core.action.ActionGroup;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class Header<K> implements Serializable, ActionGroup {
    private K id;
    protected int child = 0;
    private List<HackitTag> tags;
    private boolean has_Callback_tag = false;
    private boolean has_skip_tag = false;
    private boolean has_sendout_tag = false;
    private boolean has_haltjob_tag = false;

    public Header() {
        this.id = generateID();
    }

    public Header(K id) {
        this.id = id;
    }

    public Header(K id, int child){
        this(id);
        this.child = child;
    }


    public K getId(){
        return this.id;
    }

    public void addTag(HackitTag tag){
        if(this.tags == null){
            this.tags = new ArrayList<>();
        }
        this.tags.add(tag);
        updateActionVector(tag);
    }

    public void clearTags(){
        this.tags.clear();
        this.has_Callback_tag = false;
        this.has_haltjob_tag  = false;
        this.has_sendout_tag  = false;
        this.has_skip_tag     = false;
    }

    public Iterator<HackitTag> iterate(){
        if(this.tags == null){
            return Collections.emptyIterator();
        }
        return this.tags.iterator();
    }

    public abstract Header<K> createChild();

    protected abstract K generateID();

    @Override
    public String toString() {
        return "HackItTupleHeader{" +
                "id=" + id +
                ", child=" + child +
                '}';
    }

    private void updateActionVector(HackitTag tag){
        this.has_Callback_tag = (tag.hasCallback())? true: this.has_Callback_tag;
        this.has_haltjob_tag  = (tag.isHaltJob())? true: this.has_haltjob_tag;
        this.has_sendout_tag  = (tag.isSendOut())? true: this.has_sendout_tag;
        this.has_skip_tag     = (tag.isSkip())? true: this.has_skip_tag;
    }

    @Override
    public boolean hasCallback() {
        return this.has_Callback_tag;
    }

    @Override
    public boolean isHaltJob() {
        return this.has_haltjob_tag;
    }

    @Override
    public boolean isSendOut() {
        return this.has_sendout_tag;
    }

    @Override
    public boolean isSkip() {
        return this.has_skip_tag;
    }
}
