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

package org.apache.wayang.tensorflow.channels;

import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.tensorflow.operators.TensorflowCollectOperator;
import org.apache.wayang.tensorflow.operators.TensorflowCollectionSource;
import org.apache.wayang.tensorflow.platform.TensorflowPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s for the {@link TensorflowPlatform}.
 */
public class ChannelConversions {

    public static final ChannelConversion COLLECTION_TO_TENSOR = new DefaultChannelConversion(
            CollectionChannel.DESCRIPTOR,
            TensorChannel.DESCRIPTOR,
            () -> new TensorflowCollectionSource<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion TENSOR_TO_COLLECTION = new DefaultChannelConversion(
            TensorChannel.DESCRIPTOR,
            CollectionChannel.DESCRIPTOR,
            () -> new TensorflowCollectOperator<>(DataSetType.createDefault(Void.class))
    );

    public static Collection<ChannelConversion> ALL = Arrays.asList(
            COLLECTION_TO_TENSOR,
            TENSOR_TO_COLLECTION
    );
}
