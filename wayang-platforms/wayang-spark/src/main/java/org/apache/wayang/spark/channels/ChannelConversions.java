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

package org.apache.wayang.spark.channels;

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.operators.SparkBroadcastOperator;
import org.apache.wayang.spark.operators.SparkCacheOperator;
import org.apache.wayang.spark.operators.SparkCollectOperator;
import org.apache.wayang.spark.operators.SparkCollectionSource;
import org.apache.wayang.spark.operators.SparkObjectFileSink;
import org.apache.wayang.spark.operators.SparkObjectFileSource;
import org.apache.wayang.spark.operators.SparkTsvFileSink;
import org.apache.wayang.spark.operators.SparkTsvFileSource;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s used by the {@link JavaPlatform}.
 */
public class ChannelConversions {

    public static final ChannelConversion UNCACHED_RDD_TO_CACHED_RDD = new DefaultChannelConversion(
            RddChannel.UNCACHED_DESCRIPTOR,
            RddChannel.CACHED_DESCRIPTOR,
            () -> new SparkCacheOperator<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion COLLECTION_TO_BROADCAST = new DefaultChannelConversion(
            CollectionChannel.DESCRIPTOR,
            BroadcastChannel.DESCRIPTOR,
            () -> new SparkBroadcastOperator<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion COLLECTION_TO_UNCACHED_RDD = new DefaultChannelConversion(
            CollectionChannel.DESCRIPTOR,
            RddChannel.UNCACHED_DESCRIPTOR,
            () -> new SparkCollectionSource<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion UNCACHED_RDD_TO_COLLECTION = new DefaultChannelConversion(
            RddChannel.UNCACHED_DESCRIPTOR,
            CollectionChannel.DESCRIPTOR,
            () -> new SparkCollectOperator<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion CACHED_RDD_TO_COLLECTION = new DefaultChannelConversion(
            RddChannel.CACHED_DESCRIPTOR,
            CollectionChannel.DESCRIPTOR,
            () -> new SparkCollectOperator<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion CACHED_RDD_TO_HDFS_TSV = new DefaultChannelConversion(
            RddChannel.CACHED_DESCRIPTOR,
            FileChannel.HDFS_TSV_DESCRIPTOR,
            () -> new SparkTsvFileSink<>(DataSetType.createDefaultUnchecked(Tuple2.class))
    );

    public static final ChannelConversion UNCACHED_RDD_TO_HDFS_TSV = new DefaultChannelConversion(
            RddChannel.UNCACHED_DESCRIPTOR,
            FileChannel.HDFS_TSV_DESCRIPTOR,
            () -> new SparkTsvFileSink<>(DataSetType.createDefaultUnchecked(Tuple2.class))
    );

    public static final ChannelConversion HDFS_TSV_TO_UNCACHED_RDD = new DefaultChannelConversion(
            FileChannel.HDFS_TSV_DESCRIPTOR,
            RddChannel.UNCACHED_DESCRIPTOR,
            () -> new SparkTsvFileSource(DataSetType.createDefault(Tuple2.class))
    );

    public static final ChannelConversion CACHED_RDD_TO_HDFS_OBJECT_FILE = new DefaultChannelConversion(
            RddChannel.CACHED_DESCRIPTOR,
            FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
            () -> new SparkObjectFileSink<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion UNCACHED_RDD_TO_HDFS_OBJECT_FILE = new DefaultChannelConversion(
            RddChannel.UNCACHED_DESCRIPTOR,
            FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
            () -> new SparkObjectFileSink<>(DataSetType.createDefault(Void.class))
    );

    public static final ChannelConversion HDFS_OBJECT_FILE_TO_UNCACHED_RDD = new DefaultChannelConversion(
            FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
            RddChannel.UNCACHED_DESCRIPTOR,
            () -> new SparkObjectFileSource<>(DataSetType.createDefault(Void.class))
    );

    public static Collection<ChannelConversion> ALL = Arrays.asList(
            UNCACHED_RDD_TO_CACHED_RDD,
            COLLECTION_TO_BROADCAST,
            COLLECTION_TO_UNCACHED_RDD,
            UNCACHED_RDD_TO_COLLECTION,
            CACHED_RDD_TO_COLLECTION,
            CACHED_RDD_TO_HDFS_OBJECT_FILE,
            UNCACHED_RDD_TO_HDFS_OBJECT_FILE,
            HDFS_OBJECT_FILE_TO_UNCACHED_RDD,
//            HDFS_TSV_TO_UNCACHED_RDD,
            CACHED_RDD_TO_HDFS_TSV,
            UNCACHED_RDD_TO_HDFS_TSV
    );
}
