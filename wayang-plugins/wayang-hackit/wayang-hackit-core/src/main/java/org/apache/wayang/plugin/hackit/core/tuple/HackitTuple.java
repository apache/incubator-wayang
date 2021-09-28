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
 * out as part of the shuffle process
 *
 * HackitTuple implements {@link ActionGroup} because it could have any {@link org.apache.wayang.plugin.hackit.core.action.Action}
 * to perform, and it is easy to validate at runtime which operations need to be performed
 *
 * @param <K> type of the key that will be used as an identifier on the HackitTuple
 * @param <T> type of the element that it will be wrapped inside the HackitTuple
 */
public class HackitTuple<K, T> implements Serializable, ActionGroup {

    /**
     * BUILDER is the {@link HeaderBuilder} that produces the identifier on the process
     * of construction of the new {@link Header}
     */
    private static HeaderBuilder BUILDER;

    /**
     * header is a {@link Header}, this helps to save relevant metadata of the tuple. E.g. added Tags.
     */
    private Header<K> header;

    /**
     * value is the element that is wrapped
     */
    private T value;

    /**
     * this static creates the {@link HeaderBuilder} that will be used during the process
     * of generating the {@link Header}
     */
    static {
        //TODO: the generation of the HeaderBuilder need to be done by configuration and using maybe a Dependency Inyection
        BUILDER = new HeaderBuilder();
    }

    /**
     * Default Constructor, internally it generates the tuple
     *
     * @param value that will be wrapped by current {@link HackitTuple}
     */
    public HackitTuple(T value){
        this.header = BUILDER.generateHeader();
        this.value = value;
    }

    /**
     * Constructor where the header can be provided, instead of using the default {@link HeaderBuilder}
     *
     * @param header {@link Header} that will save the relevant metadata
     * @param value that will be wrapped by the {@link HackitTuple}
     */
    public HackitTuple(Header<K> header, T value){
        this.header = header;
        this.value = value;
    }

    /**
     * Obtain the header of the tuple
     *
     * @return {@link Header} that contains the relevant metadata
     */
    public Header<K> getHeader(){
        return this.header;
    }

    /**
     * Obtains the identifier allocated inside the {@link Header}
     *
     * @return identifier of the tuple
     */
    public K getKey(){
        return this.header.getId();
    }

    /**
     * Obtains the wrapped element inside the {@link HackitTuple}
     *
     * @return original value that was wrapped
     */
    public T getValue(){
        return this.value;
    }

    /**
     * Adds a {@link HackitTag} to the {@link Header}
     *
     * @param tag {@link HackitTag} that it will need at some point on the process
     */
    public void addTag(HackitTag tag){
        this.header.addTag(tag);
    }

    /**
     * Adds a {@link Set} of {@lin HackitTag}s on the {@link Header}
     *
     * @param tags {@link HackitTag} that will be needed at some point of the process
     */
    public void addTag(Set<HackitTag> tags){
        this.header.addTag(tags);
    }

    /**
     * Gets an {@link Iterator} of the current {@link HackitTag}s that are inside the {@link Header}
     *
     * @return {@link Iterator} of tags
     */
    public Iterator<HackitTag> getTags(){
        return this.header.iterate();
    }

    @Override
    public String toString() {
        return String.format("HackItTuple{header=%s, value=%s}", header, value);
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Callback action}
     */
    @Override
    public boolean hasCallback() {
        return this.getHeader().hasCallback();
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Halt job action}
     */
    @Override
    public boolean isHaltJob() {
        return this.getHeader().isHaltJob();
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Send out action}
     */
    @Override
    public boolean isSendOut() {
        return this.getHeader().isSendOut();
    }

    /**
     *
     * @return {@link Boolean that specifies if current tuple requires Skip action}
     */
    @Override
    public boolean isSkip() {
        return this.getHeader().isSkip();
    }
}
