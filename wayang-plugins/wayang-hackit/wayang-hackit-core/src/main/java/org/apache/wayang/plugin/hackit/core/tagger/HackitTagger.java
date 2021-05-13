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

import org.apache.wayang.plugin.hackit.core.iterator.HackitIterator;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HackitTagger is class where is allocated all the logic that need to be perform during the
 * tagging step in Hackit, this logic have and pre and post processing and they are acting like
 * template that follow same behaivor in every tagger
 */
//TODO add the option of add a custom function
public class HackitTagger implements Serializable {

    /**
     * {@link List} of {@link HackitTag} that are added previous of the execution of the
     * original function
     */
    //TODO: It may change by a set
    protected List<HackitTag> pre_tags;

    /**
     * {@link List} of {@link HackitTag} that are added after of the execution of the
     * original function
     */
    //TODO: It may change by a set
    protected List<HackitTag> post_tags;

    /**
     * Default Construct
     */
    public HackitTagger(){}

    /**
     * Add a {@link HackitTag} to {@link List} of <code>pre_tags</code>
     *
     * @param tag is a {@link HackitTag} added to change the future behavior
     * @return {@link HackitTagger} as it self reference
     */
    public HackitTagger addPreTag(HackitTag tag){
        if(this.pre_tags == null){
            this.pre_tags = new ArrayList<>();
        }
        this.pre_tags.add(tag);
        return this;
    }

    /**
     * Add a {@link HackitTag} to {@link List} of <code>post_tags</code>
     *
     * @param tag is a {@link HackitTag} added to change the future behavior
     * @return {@link HackitTagger} as it self reference
     */
    public HackitTagger addPostTag(HackitTag tag){
        if(this.post_tags == null){
            this.post_tags = new ArrayList<>();
        }
        this.post_tags.add(tag);
        return this;
    }

    /**
     * add to the {@link HackitTuple} all the {@link HackitTag}'s add pre-tagging phase are available at that moment
     *
     * @param tuple is a {@link HackitTuple} where that {@link HackitTag} will be added
     */
    public void preTaggingTuple(HackitTuple tuple){
        if(this.pre_tags != null)
            taggingTuple(tuple, this.pre_tags);
    }

    /**
     * add to the {@link HackitTuple} all the {@link HackitTag}'s add post-tagging phase are available at that moment
     *
     * @param tuple is a {@link HackitTuple} where that {@link HackitTag} will be added
     */
    public void postTaggingTuple(HackitTuple tuple){
        if(this.post_tags != null)
            taggingTuple(tuple, this.post_tags);
    }

    /**
     * add all the {@link HackitTag}'s available on the {@link List} to the {@link HackitTuple}
     *
     * @param tuple is {@link HackitTuple} where the tags will be added
     * @param tags {@link List} of {@link HackitTag}'s that will add to {@link HackitTuple}
     */
    public void taggingTuple(HackitTuple tuple, List<HackitTag> tags){
        //TODO: change this code for an efficient one
        tags.stream().forEach(tag -> tuple.addTag(tag.getInstance()));
    }

    /**
     * It take the original {@link HackitTuple} and extract the {@link Header} and start creating the children
     * from that {@link Header} this enable to follow the lineage of the after a several elements come out from
     * one {@link HackitTuple}. This generation is possible by inserting a new step at the iterator using {@link HackitIterator}
     * that allow append a new instruction in the process that will be perform on the original {@link Iterator}
     *
     * @param origin Original {@link HackitTuple} that it was transformed
     * @param result is the transformation output inside of an {@link Iterator}
     * @param <K> type of the identifier of {@link HackitTuple}
     * @param <I> type of the original element inside of {@link HackitTuple} that it was transformed
     * @param <O> type of the output in the transformation
     *
     * @return {@link Iterator} that is wrapper of the original with the add instruction using {@link HackitIterator}
     */
    public <K, I, O> Iterator<HackitTuple<K,O>> postTaggingTuple(HackitTuple<K, I> origin, Iterator<O>result){
        Header<K> header = origin.getHeader();
        Iterator<HackitTuple<K, O>> iter_result = new HackitIterator<K, O>(
                result,
                record -> {
                    HackitTuple<K, O> tuple = new HackitTuple<K, O>(
                            header.createChild(),
                            record
                    );
                    postTaggingTuple(tuple);
                    return tuple;
                }
        );
        return iter_result;
    }

    /**
     * It take the original {@link HackitTuple} and extract the {@link Header} to be enable to follow the lineage of the
     * same value transformed, then is generate a new {@link HackitTuple} with the <code>result</code> as the wrapped
     * element, it also add the new {@link HackitTag}'s to the {@link Header}
     *
     *
     * @param origin Original {@link HackitTuple} that it was transformed
     * @param result is the transformation output
     * @param <K> type of the identifier of {@link HackitTuple}
     * @param <I> type of the original element inside of {@link HackitTuple} that it was transformed
     * @param <O> type of the output in the transformation
     *
     * @return {@link HackitTuple} with the new {@link HackitTag}
     */
    public <K, I, O> HackitTuple<K, O> postTaggingTuple(HackitTuple<K, I> origin, O result){
        HackitTuple<K, O> hackItTuple_result = new HackitTuple<K, O>(origin.getHeader(), result);
        this.postTaggingTuple(hackItTuple_result);
        return hackItTuple_result;
    }
}
