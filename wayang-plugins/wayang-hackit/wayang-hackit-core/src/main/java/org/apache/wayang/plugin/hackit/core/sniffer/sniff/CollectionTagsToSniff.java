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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Sniffing {@link Sniff} use case when a Set of {@link HackitTag}s is required to evaluate HackitTuples
 */
public class CollectionTagsToSniff implements Sniff {

    /**
     * Contains the Set {@link HackitTag}s to be evaluated
     */
    public Set<HackitTag> tags2sniff;

    /**
     * Default Constructor
     */
    public CollectionTagsToSniff(){
        this.tags2sniff = new HashSet<>();
    }

    /**
     * Constructor that assigns a Set of HackitTags to Sniff tuples
     */
    public CollectionTagsToSniff(Set<HackitTag> tags2sniff){
        this.tags2sniff = tags2sniff;
    }

    /**
     *
     * @param tag to be Added in this Sniffer {@link Sniff}
     */
    public void addTag2sniff(HackitTag tag) {
        this.tags2sniff.add(tag);
    }

    /**
     * Review that at least a single preset {@link HackitTag} is present
     * on received {@link HackitTuple}
     *
     * @param input element to evaluate if is sniffable
     * @return {@link Boolean} Indicates if the Sniffer has found at least a single Tag {@link HackitTag}
     */
    @Override
    public boolean sniff(HackitTuple input) {
        Iterator<HackitTag> iterator = input.getTags();
        while (iterator.hasNext()){
            HackitTag tag = iterator.next();
            if(this.tags2sniff.contains(tag)){
                return true;
            }
        }
        return false;
    }
}
