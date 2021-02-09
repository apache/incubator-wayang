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

package org.apache.wayang.core.plugin;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.test.TestSinkMapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.test.DummyPlatform;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.WayangCollections;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * Test suite for the {@link DynamicPlugin} class.
 */
public class DynamicPluginTest {

    public static final Collection<ChannelConversion> CHANNEL_CONVERSIONS = Collections.singletonList(
            new DefaultChannelConversion(
                    mock(ChannelDescriptor.class), mock(ChannelDescriptor.class), () -> null, "Test conversion"
            )
    );

    @Test
    public void testLoadYaml() {
        final DynamicPlugin plugin =
                DynamicPlugin.loadYaml(ReflectionUtils.getResourceURL("test-plugin.yaml").toString());

        Set<Platform> expectedPlatforms = WayangCollections.asSet(DummyPlatform.getInstance());
        Assert.assertEquals(expectedPlatforms, WayangCollections.asSet(plugin.getRequiredPlatforms()));

        Set<Platform> expectedExcludedPlatforms = Collections.emptySet();
        Assert.assertEquals(expectedExcludedPlatforms, WayangCollections.asSet(plugin.getExcludedRequiredPlatforms()));

        Set<Mapping> expectedMappings = WayangCollections.asSet(new TestSinkMapping());
        Assert.assertEquals(expectedMappings, WayangCollections.asSet(plugin.getMappings()));

        Set<Mapping> expectedExcludedMappings = WayangCollections.asSet();
        Assert.assertEquals(expectedExcludedMappings, WayangCollections.asSet(plugin.getExcludedMappings()));

        Set<ChannelConversion> expectedConversions = WayangCollections.asSet();
        Assert.assertEquals(expectedConversions, WayangCollections.asSet(plugin.getChannelConversions()));

        Set<ChannelConversion> expectedExcludedConversions = WayangCollections.asSet(CHANNEL_CONVERSIONS);
        Assert.assertEquals(expectedExcludedConversions, WayangCollections.asSet(plugin.getExcludedChannelConversions()));

        Configuration configuration = new Configuration();
        plugin.setProperties(configuration);
        Assert.assertEquals(51.3d, configuration.getDoubleProperty("org.apache.wayang.test.float"), 0.000001);
        Assert.assertEquals("abcdef", configuration.getStringProperty("org.apache.wayang.test.string"));
        Assert.assertEquals(1234567890123456789L, configuration.getLongProperty("org.apache.wayang.test.long"));
    }

    @Test
    public void testPartialYaml() {
        final DynamicPlugin plugin =
                DynamicPlugin.loadYaml(ReflectionUtils.getResourceURL("partial-plugin.yaml").toString());

        Set<Platform> expectedPlatforms = WayangCollections.asSet(DummyPlatform.getInstance());
        Assert.assertEquals(expectedPlatforms, WayangCollections.asSet(plugin.getRequiredPlatforms()));

        Set<Platform> expectedExcludedPlatforms = Collections.emptySet();
        Assert.assertEquals(expectedExcludedPlatforms, WayangCollections.asSet(plugin.getExcludedRequiredPlatforms()));

        Set<Mapping> expectedMappings = WayangCollections.asSet();
        Assert.assertEquals(expectedMappings, WayangCollections.asSet(plugin.getMappings()));

        Set<Mapping> expectedExcludedMappings = WayangCollections.asSet(new TestSinkMapping());
        Assert.assertEquals(expectedExcludedMappings, WayangCollections.asSet(plugin.getExcludedMappings()));

        Set<ChannelConversion> expectedConversions = WayangCollections.asSet();
        Assert.assertEquals(expectedConversions, WayangCollections.asSet(plugin.getChannelConversions()));

        Set<ChannelConversion> expectedExcludedConversions = WayangCollections.asSet();
        Assert.assertEquals(expectedExcludedConversions, WayangCollections.asSet(plugin.getExcludedChannelConversions()));
    }

    @Test
    public void testEmptyYaml() {
        final DynamicPlugin plugin =
                DynamicPlugin.loadYaml(ReflectionUtils.getResourceURL("empty-plugin.yaml").toString());

        Set<Platform> expectedPlatforms = Collections.emptySet();
        Assert.assertEquals(expectedPlatforms, WayangCollections.asSet(plugin.getRequiredPlatforms()));

        Set<Mapping> expectedMappings = Collections.emptySet();
        Assert.assertEquals(expectedMappings, WayangCollections.asSet(plugin.getMappings()));

        Set<ChannelConversion> expectedConversions = Collections.emptySet();
        Assert.assertEquals(expectedConversions, WayangCollections.asSet(plugin.getChannelConversions()));
    }

    @Test
    public void testExclusion() {
        final TestSinkMapping mapping = new TestSinkMapping();
        Configuration configuration = new Configuration();

        final DynamicPlugin plugin1 = new DynamicPlugin();
        plugin1.addMapping(mapping);
        plugin1.configure(configuration);
        Assert.assertEquals(
                WayangCollections.asSet(mapping),
                WayangCollections.asSet(configuration.getMappingProvider().provideAll())
        );

        final DynamicPlugin plugin2 = new DynamicPlugin();
        plugin2.excludeMapping(mapping);
        plugin2.configure(configuration);
        Assert.assertEquals(
                WayangCollections.asSet(),
                WayangCollections.asSet(configuration.getMappingProvider().provideAll())
        );
    }
}
