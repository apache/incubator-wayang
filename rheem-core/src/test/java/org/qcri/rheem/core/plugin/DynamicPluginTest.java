package org.qcri.rheem.core.plugin;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.test.TestSinkMapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.test.DummyPlatform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemCollections;

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

        Set<Platform> expectedPlatforms = RheemCollections.asSet(DummyPlatform.getInstance());
        Assert.assertEquals(expectedPlatforms, RheemCollections.asSet(plugin.getRequiredPlatforms()));

        Set<Platform> expectedExcludedPlatforms = Collections.emptySet();
        Assert.assertEquals(expectedExcludedPlatforms, RheemCollections.asSet(plugin.getExcludedRequiredPlatforms()));

        Set<Mapping> expectedMappings = RheemCollections.asSet(new TestSinkMapping());
        Assert.assertEquals(expectedMappings, RheemCollections.asSet(plugin.getMappings()));

        Set<Mapping> expectedExcludedMappings = RheemCollections.asSet();
        Assert.assertEquals(expectedExcludedMappings, RheemCollections.asSet(plugin.getExcludedMappings()));

        Set<ChannelConversion> expectedConversions = RheemCollections.asSet();
        Assert.assertEquals(expectedConversions, RheemCollections.asSet(plugin.getChannelConversions()));

        Set<ChannelConversion> expectedExcludedConversions = RheemCollections.asSet(CHANNEL_CONVERSIONS);
        Assert.assertEquals(expectedExcludedConversions, RheemCollections.asSet(plugin.getExcludedChannelConversions()));

        Configuration configuration = new Configuration();
        plugin.setProperties(configuration);
        Assert.assertEquals(51.3d, configuration.getDoubleProperty("org.qcri.rheem.test.float"), 0.000001);
        Assert.assertEquals("abcdef", configuration.getStringProperty("org.qcri.rheem.test.string"));
        Assert.assertEquals(1234567890123456789L, configuration.getLongProperty("org.qcri.rheem.test.long"));
    }

    @Test
    public void testPartialYaml() {
        final DynamicPlugin plugin =
                DynamicPlugin.loadYaml(ReflectionUtils.getResourceURL("partial-plugin.yaml").toString());

        Set<Platform> expectedPlatforms = RheemCollections.asSet(DummyPlatform.getInstance());
        Assert.assertEquals(expectedPlatforms, RheemCollections.asSet(plugin.getRequiredPlatforms()));

        Set<Platform> expectedExcludedPlatforms = Collections.emptySet();
        Assert.assertEquals(expectedExcludedPlatforms, RheemCollections.asSet(plugin.getExcludedRequiredPlatforms()));

        Set<Mapping> expectedMappings = RheemCollections.asSet();
        Assert.assertEquals(expectedMappings, RheemCollections.asSet(plugin.getMappings()));

        Set<Mapping> expectedExcludedMappings = RheemCollections.asSet(new TestSinkMapping());
        Assert.assertEquals(expectedExcludedMappings, RheemCollections.asSet(plugin.getExcludedMappings()));

        Set<ChannelConversion> expectedConversions = RheemCollections.asSet();
        Assert.assertEquals(expectedConversions, RheemCollections.asSet(plugin.getChannelConversions()));

        Set<ChannelConversion> expectedExcludedConversions = RheemCollections.asSet();
        Assert.assertEquals(expectedExcludedConversions, RheemCollections.asSet(plugin.getExcludedChannelConversions()));
    }

    @Test
    public void testEmptyYaml() {
        final DynamicPlugin plugin =
                DynamicPlugin.loadYaml(ReflectionUtils.getResourceURL("empty-plugin.yaml").toString());

        Set<Platform> expectedPlatforms = Collections.emptySet();
        Assert.assertEquals(expectedPlatforms, RheemCollections.asSet(plugin.getRequiredPlatforms()));

        Set<Mapping> expectedMappings = Collections.emptySet();
        Assert.assertEquals(expectedMappings, RheemCollections.asSet(plugin.getMappings()));

        Set<ChannelConversion> expectedConversions = Collections.emptySet();
        Assert.assertEquals(expectedConversions, RheemCollections.asSet(plugin.getChannelConversions()));
    }

    @Test
    public void testExclusion() {
        final TestSinkMapping mapping = new TestSinkMapping();
        Configuration configuration = new Configuration();

        final DynamicPlugin plugin1 = new DynamicPlugin();
        plugin1.addMapping(mapping);
        plugin1.configure(configuration);
        Assert.assertEquals(
                RheemCollections.asSet(mapping),
                RheemCollections.asSet(configuration.getMappingProvider().provideAll())
        );

        final DynamicPlugin plugin2 = new DynamicPlugin();
        plugin2.excludeMapping(mapping);
        plugin2.configure(configuration);
        Assert.assertEquals(
                RheemCollections.asSet(),
                RheemCollections.asSet(configuration.getMappingProvider().provideAll())
        );
    }
}
