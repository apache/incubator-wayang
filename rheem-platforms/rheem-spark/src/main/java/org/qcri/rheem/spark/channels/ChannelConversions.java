package org.qcri.rheem.spark.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.operators.SparkBroadcastOperator;
import org.qcri.rheem.spark.operators.SparkCacheOperator;
import org.qcri.rheem.spark.operators.SparkCollectOperator;
import org.qcri.rheem.spark.operators.SparkCollectionSource;
import org.qcri.rheem.spark.operators.SparkObjectFileSink;
import org.qcri.rheem.spark.operators.SparkObjectFileSource;
import org.qcri.rheem.spark.operators.SparkTsvFileSink;
import org.qcri.rheem.spark.operators.SparkTsvFileSource;

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
