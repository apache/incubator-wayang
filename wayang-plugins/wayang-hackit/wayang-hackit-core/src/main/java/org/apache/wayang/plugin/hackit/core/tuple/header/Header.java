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

/**
 * Header is the container of the metadata asociated to one {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 *
 * @param <K> type of the identifier of the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 */
public abstract class Header<K> implements Serializable, ActionGroup {

    /**
     * id is identifier of the header and also the tuple
     */
    private K id;

    /**
     * child indicate the number of the child that the current header is from the
     * original {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple} and they share the
     * same identifier <code>id</code>
     */
    protected int child = 0;

    /**
     * tags added to the header, this describe some action that need to be apply to the
     * {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
     */
    private List<HackitTag> tags;

    /**
     * during the process of adding news {@link HackitTag} could add a new {@link org.apache.wayang.plugin.hackit.core.action.Action} at the header, and this
     * change the status of <code>has_callback_tag</code>
     */
    private boolean has_callback_tag = false;

    /**
     * during the process of adding news {@link HackitTag} could add a new {@link org.apache.wayang.plugin.hackit.core.action.Action} at the header, and this
     * change the status of <code>has_skip_tag</code>
     */
    private boolean has_skip_tag = false;

    /**
     * during the process of adding news {@link HackitTag} could add a new {@link org.apache.wayang.plugin.hackit.core.action.Action} at the header, and this
     * change the status of <code>has_sendout_tag</code>
     */
    private boolean has_sendout_tag = false;

    /**
     * during the process of adding news {@link HackitTag} could add a new {@link org.apache.wayang.plugin.hackit.core.action.Action} at the header, and this
     * change the status of <code>has_haltjob_tag</code>
     */
    private boolean has_haltjob_tag = false;

    /**
     * Default Construct, this will call {@link #generateID()} and produce the new identifier
     */
    public Header() {
        this.id = generateID();
    }

    /**
     * Construct with the identifier as parameter
     *
     * @param id is the identifier of the Header
     */
    public Header(K id) {
        this.id = id;
    }

    /**
     * Construct with the identifier and child identifier as parameter
     *
     * @param id is the identifier of the Header
     * @param child is the child identifier assigned
     */
    public Header(K id, int child){
        this(id);
        this.child = child;
    }

    /**
     * retrieve the identifier of the Header
     *
     * @return current identifier of type <code>K</code>
     */
    public K getId(){
        return this.id;
    }

    /**
     * Add a {@link HackitTag} that could provide a new {@link org.apache.wayang.plugin.hackit.core.action.Action} to be
     * perfomed by Hackit to the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}, also update all the
     * possible action calling the method {@link #updateActionVector(HackitTag)}
     *
     * @param tag {@link HackitTag}
     */
    public void addTag(HackitTag tag){
        //TODO: could be better to use an Set because it just saving uniques elements
        if(this.tags == null){
            this.tags = new ArrayList<>();
        }
        this.tags.add(tag);
        //update all the possible actions on the {@link ActionGroup}
        //TODO: just execute this action when the element is inserted the first time
        updateActionVector(tag);
    }

    /**
     * remove all the tags from the header, and set all the possible options as false
     */
    public void clearTags(){
        this.tags.clear();
        this.has_callback_tag = false;
        this.has_haltjob_tag  = false;
        this.has_sendout_tag  = false;
        this.has_skip_tag     = false;
    }

    /**
     * iterate provide an {@link Iterator} that contains all the {@link HackitTag} that were
     * add on the {@link #tags}
     *
     * If the {@link #tags} is null or empty it will return an {@link Collections#emptyIterator()}
     *
     * @return {@link Iterator} with the current {@link HackitTag}'s
     */
    public Iterator<HackitTag> iterate(){
        //TODO: maybe is need to add the option of empty
        if(this.tags == null){
            return Collections.emptyIterator();
        }
        return this.tags.iterator();
    }

    /**
     * Generate a new header that it related on some way with the father header,
     * depending on the logic of the extender it will be way of generate the child
     *
     *
     * @return new {@link Header} that correspond to the child
     */
    public abstract Header<K> createChild();

    /**
     * Generate a new identifier of type <code>K</code> that it will use inside of {@link #Header()}
     *
     * @return new identifier of type <code>K</code>
     */
    protected abstract K generateID();

    @Override
    public String toString() {
        //TODO: maybe is better to change to String.format
        return "HackItTupleHeader{" +
                "id=" + id +
                ", child=" + child +
                '}';
    }

    /**
     * Update the possible {@link org.apache.wayang.plugin.hackit.core.action.Action} that could be perfomed
     * over the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}, this is depending of the new
     * {@link HackitTag}
     *
     * @param tag {@link HackitTag} that could have new {@link org.apache.wayang.plugin.hackit.core.action.Action}'s
     */
    private void updateActionVector(HackitTag tag){
        this.has_callback_tag = tag.hasCallback() || this.has_callback_tag;
        this.has_haltjob_tag  = tag.isHaltJob() || this.has_haltjob_tag;
        this.has_sendout_tag  = tag.isSendOut() || this.has_sendout_tag;
        this.has_skip_tag     = tag.isSkip() || this.has_skip_tag;
    }

    @Override
    public boolean hasCallback() {
        return this.has_callback_tag;
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
