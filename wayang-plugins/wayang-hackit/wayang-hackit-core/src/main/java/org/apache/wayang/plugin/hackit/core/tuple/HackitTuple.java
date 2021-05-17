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
package org.apache.wayang.plugin.hackit.core.tuple;

import org.apache.wayang.plugin.hackit.core.action.ActionGroup;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;
import org.apache.wayang.plugin.hackit.core.tuple.header.HeaderBuilder;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

/**
 * HackitTuple is the base of Hackit because is the basic structure where the needed elements are added
 * to enable the execution of the logic in the internal pipeline of hackit
 *
 * HackitTuple implements {@link Serializable} because the HackitTuple and the <code>T</code> will sent
 * out or part of the suffle process
 *
 * HackitTuple implements {@link ActionGroup} because it could have any {@link org.apache.wayang.plugin.hackit.core.action.Action}
 * to perform, and is easy to validate at runtime what are the operation that need to be performed
 *
 * @param <K> type of the key that will use as indentifier on the HackitTuple
 * @param <T> type of the element that it will be wrapper inside of the HackitTuple
 */
public class HackitTuple<K, T> implements Serializable, ActionGroup {

    /**
     * BUILDER is the {@link HeaderBuilder} that produce the identifier on the process
     * of construction the new {@link Header}
     */
    private static HeaderBuilder BUILDER;

    /**
     * header is an {@link Header}, this help to save relevant meta data of the tuple
     */
    private Header<K> header;

    /**
     * value is the element that is wrapped
     */
    private T value;

    /**
     * this static create the {@link HeaderBuilder} that will be use during the process
     * of geneating the {@link Header}
     */
    static {
        //TODO: the generation of the HeaderBuilder need to be done by configuration and using maybe a Dependency Inyection
        BUILDER = new HeaderBuilder();
    }

    /**
     * Default Construct, internally it generate the tuple
     *
     * @param value that it will wrapper by the {@link HackitTuple}
     */
    public HackitTuple(T value){
        this.header = BUILDER.generateHeader();
        this.value = value;
    }

    /**
     * Construct where the header could be provided an not use the default {@link HeaderBuilder}
     *
     * @param header {@link Header} that will be save the relevant metadata
     * @param value that it will wrapper by the {@link HackitTuple}
     */
    public HackitTuple(Header<K> header, T value){
        this.header = header;
        this.value = value;
    }

    /**
     * obtain the header of the tuple
     *
     * @return {@link Header} that contains the relevant metadata
     */
    public Header<K> getHeader(){
        return this.header;
    }

    /**
     * obtains the identifier allocated inside of the {@link Header}
     *
     * @return identifier of the tuple
     */
    public K getKey(){
        return this.header.getId();
    }

    /**
     * obtain the wrapped element inside of the {@link HackitTuple}
     *
     * @return original value that was wrapped
     */
    public T getValue(){
        return this.value;
    }

    /**
     * add an {@link HackitTag} to the {@link Header}
     *
     * @param tag {@link HackitTag} that it will need at some point on the process
     */
    public void addTag(HackitTag tag){
        this.header.addTag(tag);
    }

    /**
     * add a {@link Set} of {@lin HackitTag} on the {@link Header}
     *
     * @param tags {@link HackitTag} that it will need at some point on the process
     */
    public void addTag(Set<HackitTag> tags){
        this.header.addTag(tags);
    }

    /**
     * get a {@link Iterator} of the currents {@link HackitTag} that are inside of the {@link Header}
     * @return {@link Iterator} of tags
     */
    public Iterator<HackitTag> getTags(){
        return this.header.iterate();
    }

    @Override
    public String toString() {
        //TODO: change to String.format
        return "HackItTuple{" +
                "header=" + header +
                ", value=" + value +
                '}';
    }


    @Override
    public boolean hasCallback() {
        return this.getHeader().hasCallback();
    }

    @Override
    public boolean isHaltJob() {
        return this.getHeader().isHaltJob();
    }

    @Override
    public boolean isSendOut() {
        return this.getHeader().isSendOut();
    }

    @Override
    public boolean isSkip() {
        return this.getHeader().isSkip();
    }
}
