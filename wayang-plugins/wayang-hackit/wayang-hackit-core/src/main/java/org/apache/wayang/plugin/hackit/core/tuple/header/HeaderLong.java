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

import java.util.Map;

/**
 * HeaderLong extend {@link Header}. It is just a simple implementation with correlative numbers
 */
public class HeaderLong extends Header<Long> {

    /**
     * base is the current correlative that is used in this generator
     */
    static long base;

    static{

        base = 0;
                //HeaderLong.getConfiguration().getLongProperty("wayang.hackit.core.headerlong.correlative.base");//0 or (new Random()).nextLong();
    }

    /**
     * Default Construct
     */
    public HeaderLong(Map<String, String> configuration) {
        super();
        base = Long.parseLong(configuration.get("correlative.base"));
    }

    /**
     * Constructor where it is possible to define the identifier
     *
     * @param id is the identifier that the {@link Header} will use
     */
    public HeaderLong(Long id){
        super(id);
    }

    /**
     * Constructor where it is possible to define the identifier and the child number
     * This constructor assigns an identifier for the tuple relative to its parent
     *
     * @param id is the identifier that the {@link Header} will use
     * @param child is the identifier as child of the original element giving by <code>id</code>
     */
    public HeaderLong(Long id, int child) {
        super(id, child);
    }

    /**
     * Generates a new header {@link HeaderLong} that will be related to the father's header
     * of this tuple {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}.
     * Regarding the logic of this class, children will be differentiated by a correlative long
     *
     * @return a {@link HeaderLong} that corresponds to the current child
     */
    @Override
    public HeaderLong createChild() {
        return new HeaderLong(this.getId(), this.child++);
    }

    /**
     *
     * @return {@link Long} as an ID for the current Child
     */
    @Override
    protected Long generateID() {
        return base++;
    }
}
