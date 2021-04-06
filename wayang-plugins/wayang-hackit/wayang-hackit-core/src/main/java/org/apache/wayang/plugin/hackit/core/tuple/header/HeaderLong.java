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

public class HeaderLong extends Header<Long> {
    static long base;

    static{
        base = 0;//(new Random()).nextLong();
    }

    public HeaderLong(Long id){
        super(id);
    }

    public HeaderLong() {
        super();
    }

    public HeaderLong(Long id, int child) {
        super(id, child);
    }

    @Override
    public HeaderLong createChild() {
        return new HeaderLong(this.getId(), this.child++);
    }

    @Override
    protected Long generateID() {
        return base++;
    }
}
