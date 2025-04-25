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

package org.apache.wayang.java.operators;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.basic.operators.KafkaTopicSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Locale;
import java.util.stream.Stream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test suite for {@link JavaKafkaTopicSink}.
 */
class JavaKafkaTopicSinkTest extends JavaExecutionOperatorTestBase {

    private static final Logger logger = LoggerFactory.getLogger(JavaTextFileSinkTest.class);

    private Locale defaultLocale;

    /**
     * In locales, where the decimal separator is not "." this rest would fail.
     * Therefore we ensure it's run in a pre-defined locale and we make sure it's
     * reset after the test.
     */
    @BeforeEach
    void setupTest() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterEach
    void teardownTest() {
        Locale.setDefault(defaultLocale);
    }


    @Disabled
    @Test
    void testWritingToKafkaTopic() throws Exception {

        Configuration configuration = new Configuration();

        // We assume, that we write back into the same cluster, to avoid "external copies"...
        Properties props = KafkaTopicSource.getDefaultProperties();

        logger.info(">>> Test: testWriteIntoKafkaTopic()");

        final String topicName1 = "banking-tx-small-csv";

        logger.info("> 0 ... ");

        logger.info( "*** [TOPIC-Name] " + topicName1 + " ***");

        logger.info( ">   Write to topic ... ");

        logger.info("> 1 ... ");

        props.list(System.out);

        logger.info("> 2 ... ");

        JavaExecutor javaExecutor = null;

        try {

            JavaKafkaTopicSink<Float> sink = new JavaKafkaTopicSink<>(
                    topicName1,
                    new TransformationDescriptor<>(
                            f -> String.format("%.2f", f),
                            Float.class, String.class
                    )
            );

            logger.info("> 3 ... ");

            Job job = mock(Job.class);
            when(job.getConfiguration()).thenReturn(configuration);
            javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

            StreamChannel.Instance inputChannelInstance = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                    .createChannel(mock(OutputSlot.class), configuration)
                    .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);
            inputChannelInstance.accept(Stream.of(1.123f, -0.1f, 3f));
            evaluate(sink, new ChannelInstance[]{inputChannelInstance}, new ChannelInstance[0]);

            logger.info("> 4 ... ");

        }
        catch (Exception ex ) {

            ex.printStackTrace();

            logger.info("##5## ... ");

            fail();

        }

        assertTrue( true );

        logger.info("> *6*");


    }



}
