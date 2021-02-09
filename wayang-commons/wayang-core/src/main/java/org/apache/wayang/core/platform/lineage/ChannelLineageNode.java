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

package org.apache.wayang.core.platform.lineage;

import org.apache.wayang.core.platform.ChannelInstance;

/**
 * Encapsulates a {@link ChannelInstance} in the lazy execution lineage.
 */
public class ChannelLineageNode extends LazyExecutionLineageNode {


    /**
     * The encapsulated {@link ChannelInstance}.
     */
    private final ChannelInstance channelInstance;

    public ChannelLineageNode(final ChannelInstance channelInstance) {
        assert !channelInstance.wasProduced();
        this.channelInstance = channelInstance;
        this.channelInstance.noteObtainedReference();
    }

    @Override
    protected <T> T accept(T accumulator, Aggregator<T> aggregator) {
        return aggregator.aggregate(accumulator, this);
    }


    @Override
    protected void markAsExecuted() {
        super.markAsExecuted();
        assert !this.channelInstance.wasProduced();
        this.channelInstance.markProduced();
        this.channelInstance.noteDiscardedReference(false);
    }

    @Override
    public String toString() {
        return "ChannelLineageNode[" + channelInstance + ']';
    }

    /**
     * Retrieve the encapsulated {@link ChannelInstance}.
     *
     * @return the {@link ChannelInstance}
     */
    public ChannelInstance getChannelInstance() {
        return this.channelInstance;
    }
}
