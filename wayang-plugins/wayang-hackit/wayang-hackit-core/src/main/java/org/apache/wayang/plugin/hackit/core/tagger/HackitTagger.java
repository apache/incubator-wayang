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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * HackitTagger is a class in which all the logic that needs to be performed during the tagging step in Hackit is assigned,
 * this logic has pre and post processing, both acting as a template that follows the same behavior on each tagger
 */
//TODO add the option of add a custom function
public class HackitTagger implements Serializable {

    /**
     * {@link List} of {@link HackitTag} that are added before the original function execution
     */
    protected Set<HackitTag> pre_tags;

    /**
     * {@link List} of {@link HackitTag} that are added after the original function execution
     */
    protected Set<HackitTag> post_tags;

    /**
     * Default Construct
     */
    public HackitTagger(){}

    /**
     * Add a {@link HackitTag} to {@link List} of <code>pre_tags</code>
     *
     * @param tag is a {@link HackitTag} added to change the future behavior
     * @return {@link HackitTagger} as a self reference
     */
    public HackitTagger addPreTag(HackitTag tag){
        if(this.pre_tags == null){
            this.pre_tags = new HashSet<>();
        }
        this.pre_tags.add(tag);
        return this;
    }

    /**
     * Add a {@link HackitTag} to {@link List} of <code>post_tags</code>
     *
     * @param tag is a {@link HackitTag} added to change the future behavior
     * @return {@link HackitTagger} as a self reference
     */
    public HackitTagger addPostTag(HackitTag tag){
        if(this.post_tags == null){
            this.post_tags = new HashSet<>();
        }
        this.post_tags.add(tag);
        return this;
    }

    /**
     * Add to the {@link HackitTuple} all the {@link HackitTag}'s that are available at the moment. Add to pre-tagging phase
     *
     * @param tuple is a {@link HackitTuple} where {@link HackitTag}s will be added
     */
    public void preTaggingTuple(HackitTuple tuple){
        if(this.pre_tags != null)
            taggingTuple(tuple, this.pre_tags);
    }

    /**
     * Add to the {@link HackitTuple} all the {@link HackitTag}s that are available at the moment. Add to post-tagging phase
     *
     * @param tuple is a {@link HackitTuple} where {@link HackitTag}s will be added
     */
    public void postTaggingTuple(HackitTuple tuple){
        if(this.post_tags != null)
            taggingTuple(tuple, this.post_tags);
    }

    /**
     * Add all the {@link HackitTag}s available on the {@link List} to the {@link HackitTuple}
     *
     * @param tuple is {@link HackitTuple} where the tags will be added
     * @param tags {@link List} of {@link HackitTag}s to be added to {@link HackitTuple}
     */
    public void taggingTuple(HackitTuple tuple, Set<HackitTag> tags){
        //TODO: change this code for an efficient one
        tuple.addTag(tags);
    }

    /**
     *
     * It takes the original {@link HackitTuple}, extracts the {@link Header} and start creating children
     * from that {@link Header}. This allows the lineage to be tracked after multiple items come out of a
     * {@link HackitTuple}. This generation is possible by inserting a new step at the iterator using {@link HackitIterator}
     * that allows appending a new instruction in the process that will be performed on the original {@link Iterator}
     *
     * @param origin Original {@link HackitTuple} that was transformed
     * @param result is the transformation output inside an {@link Iterator}
     * @param <K> type of the identifier of {@link HackitTuple}
     * @param <I> type of the original element inside of {@link HackitTuple} that was transformed
     * @param <O> type of the output in the transformation
     *
     * @return {@link Iterator} wrapper of the original with an added instruction using {@link HackitIterator}
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
     * It takes the original {@link HackitTuple} and extract the {@link Header} to allow the lineage to be tracked regarding the
     * same value transformed. Then, it is generated a new {@link HackitTuple} with the <code>result</code> as the wrapped
     * element, it also adds the new {@link HackitTag}'s to the {@link Header}
     *
     * @param origin Original {@link HackitTuple} that was transformed
     * @param result is the transformation output
     * @param <K> type of the identifier of {@link HackitTuple}
     * @param <I> type of the original element inside of {@link HackitTuple} that was transformed
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
