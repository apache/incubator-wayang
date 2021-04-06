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

public class HackitTagger implements Serializable {

    protected List<HackitTag> pre_tags;
    protected List<HackitTag> post_tags;

    public HackitTagger(){}

    public HackitTagger addPreTag(HackitTag tag){
        if(this.pre_tags == null){
            this.pre_tags = new ArrayList<>();
        }
        this.pre_tags.add(tag);
        return this;
    }

    public HackitTagger addPostTag(HackitTag tag){
        if(this.post_tags == null){
            this.post_tags = new ArrayList<>();
        }
        this.post_tags.add(tag);
        return this;
    }

    public void preTaggingTuple(HackitTuple tuple){
        if(this.pre_tags != null)
            taggingTuple(tuple, this.pre_tags);
    }

    public void postTaggingTuple(HackitTuple tuple){
        if(this.post_tags != null)
            taggingTuple(tuple, this.post_tags);
    }

    public void taggingTuple(HackitTuple tuple, List<HackitTag> tags){
        tags.stream().forEach(tag -> tuple.addTag(tag.getInstance()));
    }

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

    public <K, I, O> HackitTuple<K, O> postTaggingTuple(HackitTuple<K, I> origin, O result){
        HackitTuple<K, O> hackItTuple_result = new HackitTuple<K, O>(origin.getHeader(), result);
        this.postTaggingTuple(hackItTuple_result);
        return hackItTuple_result;
    }
}
