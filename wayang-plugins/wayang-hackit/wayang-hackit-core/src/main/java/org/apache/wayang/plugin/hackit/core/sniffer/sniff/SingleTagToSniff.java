/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.plugin.hackit.core.sniffer.sniff;

import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Iterator;

/**
 * Sniffing {@link Sniff} use case when a single {@link HackitTag} is required to evaluate HackitTuples
 */
public class SingleTagToSniff implements Sniff {

    /**
     * Contains the single {@link HackitTag} to be evaluated
     */
    public HackitTag tag2sniff;

    /**
     * Default constructor
     */
    public SingleTagToSniff(){
        this.tag2sniff = null;
    }

    /**
     * Constructor that sets the Tag
     */
    public SingleTagToSniff(HackitTag tag){
        this.tag2sniff = tag;
    }

    /**
     *
     * @param tag to be Added in this Sniffer {@link Sniff}
     */
    public void addTag2sniff(HackitTag tag) {
        if(this.tag2sniff != null){
            throw new RuntimeException("The SingleTagToSniff already got the tag, if you need more of one tag use CollectionTagsToSniff class");
        }
        this.tag2sniff = tag;
    }

    /**
     * Returns whether the received {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
     * contains the preset tag in this Sniffer {@link Sniff}
     *
     * @param input {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple} analyzed to match its tags with the Sniffer preset tag
     * @return {@link Boolean} Indicates if the Sniffer has found the Tag {@link HackitTag}
     */
    @Override
    public boolean sniff(HackitTuple input) {
        Iterator<HackitTag> iterator = input.getTags();
        while (iterator.hasNext()){
            HackitTag tag = iterator.next();
            if(tag.equals(this.tag2sniff)){
                return true;
            }
        }
        return false;
    }
}
