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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Header is the container of the metadata associated to one {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 *
 * @param <K> type of the identifier of the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 */
public abstract class Header<K> implements Serializable, ActionGroup {

    /**
     * id is identifier of the header and also the tuple
     */
    private K id;

    /**
     * Child indicates the number of the child corresponding to the current header, regarding the
     * original (parent) {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}.
     * Current child and Original (parent) have the same identifier <code>id</code>
     */
    protected int child = 0;

    /**
     * Tags added to the header, each tag describes some action that needs to be applied to the
     * {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
     */
    private Set<HackitTag> tags;

    /**
     * During the process of adding new {@link HackitTag}s could be added new {@link org.apache.wayang.plugin.hackit.core.action.Action}s
     * at the header. This change the status of <code>has_callback_tag</code>
     */
    private boolean has_callback_tag = false;

    /**
     * During the process of adding new {@link HackitTag}s could be added new {@link org.apache.wayang.plugin.hackit.core.action.Action}s
     * at the header. This change the status of <code>has_skip_tag</code>
     */
    private boolean has_skip_tag = false;

    /**
     * During the process of adding new {@link HackitTag}s could be added new {@link org.apache.wayang.plugin.hackit.core.action.Action}s
     * at the header. This change the status of <code>has_sendout_tag</code>
     */
    private boolean has_sendout_tag = false;

    /**
     * During the process of adding new {@link HackitTag}s could be added new {@link org.apache.wayang.plugin.hackit.core.action.Action}s
     * at the header. This change the status of <code>has_haltjob_tag</code>
     */
    private boolean has_haltjob_tag = false;

    /**
     * Default Constructor, this will call {@link #generateID()} and produce the new identifier
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
     * Constructor with identifier and child identifier as parameter
     *
     * @param id is the identifier of the Header
     * @param child is the child identifier assigned
     */
    public Header(K id, int child){
        this(id);
        this.child = child;
    }

    /**
     * Retrieve the identifier of the Header and subsequently the identifier of the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
     *
     * @return current identifier of type <code>K</code>
     */
    public K getId(){
        return this.id;
    }

    /**
     * Add a {@link HackitTag} that could provide a new {@link org.apache.wayang.plugin.hackit.core.action.Action} to be
     * performed by Hackit to the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}, also update all the
     * possible actions calling the method {@link #updateActionVector(HackitTag)}
     *
     * @param tag {@link HackitTag}
     */
    public void addTag(HackitTag tag){

        if(this.tags == null){
            this.tags = new HashSet<>();
        }

        //update all the possible actions on the {@link ActionGroup}
        if(this.tags.add(tag)){
            updateActionVector(tag);
        }
        ;
    }

    /**
     * Does exactly the same as {@link #addTag(HackitTag)}, but adding all the elements of the Set at the same time
     *
     * @param tags {@link Set} of {@link HackitTag} that will be added
     */
    public void addTag(Set<HackitTag> tags){
        if(this.tags == null){
            this.tags = new HashSet<>();
        }
        this.tags.addAll(tags);
        tags.stream()
            .forEach(
                this::updateActionVector
            )
        ;
    }

    /**
     * Remove all {@link org.apache.wayang.plugin.hackit.core.tuple.header.Header}'s tags and set all possible options to false
     */
    public void clearTags(){
        this.tags.clear();
        this.has_callback_tag = false;
        this.has_haltjob_tag  = false;
        this.has_sendout_tag  = false;
        this.has_skip_tag     = false;
    }

    /**
     * Iterate provides an {@link Iterator} that contains all the {@link HackitTag}s that currently exists in {@link #tags}
     *
     * If {@link #tags} is null or empty, it will return an {@link Collections#emptyIterator()}
     *
     * @return {@link Iterator} with the current {@link HackitTag}s
     */
    public Iterator<HackitTag> iterate(){

        if(this.tags == null || this.tags.isEmpty()){
            return Collections.emptyIterator();
        }
        return this.tags.iterator();
    }

    /**
     * Generates a new header {@link Header} that will be related to the father's header
     * of this tuple {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}.
     * Depending on the logic of the extender of this class, it will define a way to generate this child header {@link Header}
     *
     *
     * @return new {@link Header} that corresponds to the child
     */
    public abstract Header<K> createChild();

    /**
     * Generates a new identifier of type <code>K</code> that will be used inside of {@link #Header()}
     *
     * @return new identifier of type <code>K</code>
     */
    protected abstract K generateID();

    @Override
    public String toString() {
        return String.format("HackItTupleHeader{id=%s, child=%s}", id, child);
    }

    /**
     * Updates the possible {@link org.apache.wayang.plugin.hackit.core.action.Action} that could be performed
     * over the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}, this depends on the new
     * {@link HackitTag}
     *
     * @param tag {@link HackitTag} that could require new {@link org.apache.wayang.plugin.hackit.core.action.Action}s
     */
    private void updateActionVector(HackitTag tag){
        this.has_callback_tag = tag.hasCallback() || this.has_callback_tag;
        this.has_haltjob_tag  = tag.isHaltJob() || this.has_haltjob_tag;
        this.has_sendout_tag  = tag.isSendOut() || this.has_sendout_tag;
        this.has_skip_tag     = tag.isSkip() || this.has_skip_tag;
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Callback action}
     */
    @Override
    public boolean hasCallback() {
        return this.has_callback_tag;
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Halt job action}
     */
    @Override
    public boolean isHaltJob() {
        return this.has_haltjob_tag;
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Send out action}
     */
    @Override
    public boolean isSendOut() {
        return this.has_sendout_tag;
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Skip action}
     */
    @Override
    public boolean isSkip() {
        return this.has_skip_tag;
    }
}
