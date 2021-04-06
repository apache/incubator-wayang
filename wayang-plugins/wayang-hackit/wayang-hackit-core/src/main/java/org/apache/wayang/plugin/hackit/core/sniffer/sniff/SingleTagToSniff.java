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

public class SingleTagToSniff implements Sniff {

    public HackitTag tags2sniff;

    public boolean sniff(HackitTag tag){
        return this.tags2sniff.equals(tag);
    }

    public void addTag2sniff(HackitTag tag) {
        if(this.tags2sniff != null){
            throw new RuntimeException("The SingleTagToSniff already got the tag, if you need more of one tag use CollectionTagsToSniff class");
        }
        this.tags2sniff = tag;
    }

    @Override
    public boolean sniff(Object input) {
        return false;
    }
}
