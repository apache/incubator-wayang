package org.apache.incubator.wayang.core.plugin;

import org.junit.Assert;
import org.junit.Test;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.test.TestSinkMapping;
import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.test.DummyPlatform;
import org.apache.incubator.wayang.core.util.ReflectionUtils;
import org.apache.incubator.wayang.core.util.WayangCollections;

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
        Assert.assertEquals(51.3d, configuration.getDoubleProperty("org.apache.incubator.wayang.test.float"), 0.000001);
        Assert.assertEquals("abcdef", configuration.getStringProperty("org.apache.incubator.wayang.test.string"));
        Assert.assertEquals(1234567890123456789L, configuration.getLongProperty("org.apache.incubator.wayang.test.long"));
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
