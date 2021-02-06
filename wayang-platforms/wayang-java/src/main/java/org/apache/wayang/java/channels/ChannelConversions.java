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

package org.apache.wayang.java.channels;

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaCollectOperator;
import org.apache.wayang.java.operators.JavaObjectFileSink;
import org.apache.wayang.java.operators.JavaObjectFileSource;
import org.apache.wayang.java.operators.JavaTsvFileSink;
import org.apache.wayang.java.operators.JavaTsvFileSource;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s for the {@link JavaPlatform}.
 */
public class ChannelConversions {

    public static final ChannelConversion STREAM_TO_COLLECTION = new DefaultChannelConversion(
            StreamChannel.DESCRIPTOR,
            CollectionChannel.DESCRIPTOR,
            () -> new JavaCollectOperator<>(DataSetType.createDefault(Void.class))
    );

    // We could add a COLLECTION_TO_STREAM conversion, but it would probably never be used.

    public static final ChannelConversion STREAM_TO_HDFS_TSV = new DefaultChannelConversion(
            StreamChannel.DESCRIPTOR,
            FileChannel.HDFS_TSV_DESCRIPTOR,
            () -> new JavaTsvFileSink<>(DataSetType.createDefaultUnchecked(Tuple2.class))
    );

    public static final ChannelConversion COLLECTION_TO_HDFS_TSV = new DefaultChannelConversion(
            CollectionChannel.DESCRIPTOR,
            FileChannel.HDFS_TSV_DESCRIPTOR,
            () -> new JavaTsvFileSink<>(DataSetType.createDefaultUnchecked(Tuple2.class))
    );

    public static final ChannelConversion HDFS_TSV_TO_STREAM = new DefaultChannelConversion(
            FileChannel.HDFS_TSV_DESCRIPTOR,
            StreamChannel.DESCRIPTOR,
            () -> new JavaTsvFileSource<>(DataSetType.createDefault(Tuple2.class))
    );

    public static final ChannelConversion STREAM_TO_HDFS_OBJECT_FILE = new DefaultChannelConversion(
            StreamChannel.DESCRIPTOR,
            FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
            () -> new JavaObjectFileSink<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion COLLECTION_TO_HDFS_OBJECT_FILE = new DefaultChannelConversion(
            CollectionChannel.DESCRIPTOR,
            FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
            () -> new JavaObjectFileSink<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion HDFS_OBJECT_FILE_TO_STREAM = new DefaultChannelConversion(
            FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
            StreamChannel.DESCRIPTOR,
            () -> new JavaObjectFileSource<>(DataSetType.createDefault(Void.class))
    );

    public static Collection<ChannelConversion> ALL = Arrays.asList(
            STREAM_TO_COLLECTION,
            STREAM_TO_HDFS_OBJECT_FILE,
            COLLECTION_TO_HDFS_OBJECT_FILE,
            HDFS_OBJECT_FILE_TO_STREAM,
//            HDFS_TSV_TO_STREAM,
            STREAM_TO_HDFS_TSV,
            COLLECTION_TO_HDFS_TSV
    );
}
