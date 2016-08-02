package org.qcri.rheem.core.plugin;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * This {@link Plugin} can be arbitrarily customized.
 */
public class DynamicPlugin implements Plugin {

    private final Set<Platform> requiredPlatforms = new HashSet<>();

    private final Set<Mapping> mappings = new HashSet<>();

    private final Set<ChannelConversion> channelConversions = new HashSet<>();

    private final Map<String, String> properties = new HashMap<>();

    /**
     * Loads a YAML file of the following format:
     * <pre>{@code
     * # mappings to be loaded by this plugin
     * mappings:
     *   - <expression that evaluates to a Mapping instance or Collection of such>
     *
     * # channel conversions to be loaded by this plugin
     * conversions:
     *   - <expression that evaulates to a ChannelConversion instance or Collection of such>
     *
     * # properties configured by this plugin
     * properties:
     *   <property name>: <property value>
     *
     * # required platforms
     * platforms:
     *   - <expression that evaluates to a Platform instance or Collection of such>
     * }</pre>
     *
     * @param yamlUrl an URL to a YAML file of the above format
     * @return the configured instance
     * @see ReflectionUtils#evaluate(String) Supported expressions
     */
    public static DynamicPlugin loadYaml(String yamlUrl) {
        // Load YAML file.
        final FileSystem fileSystem = FileSystems.getFileSystem(yamlUrl).orElseThrow(
                () -> new RheemException(String.format("No filesystem for %s.", yamlUrl))
        );
        Object yaml;
        try (final InputStream inputStream = fileSystem.open(yamlUrl)) {
            yaml = new Yaml().load(inputStream);
        } catch (IOException e) {
            throw new RheemException(String.format("Could not load %s.", yamlUrl));
        }

        DynamicPlugin plugin = new DynamicPlugin();
        try {
            // Evaluate YAML file.
            Map<String, Object> values = asInstanceOf(yaml, Map.class);
            if (values != null) {
                // Platforms.
                List<String> platformExpressions = asInstanceOf(values.get("platforms"), List.class);
                if (platformExpressions != null) {
                    for (String platformExpression : platformExpressions) {
                        Object eval = ReflectionUtils.evaluate(platformExpression);
                        if (eval instanceof Platform) {
                            plugin.addRequiredPlatform((Platform) eval);
                        } else {
                            Collection<?> platforms = (Collection<?>) eval;
                            for (Object platform : platforms) {
                                plugin.addRequiredPlatform((Platform) platform);
                            }
                        }
                    }
                }

                // Mappings.
                List<String> mappingExpressions = asInstanceOf(values.get("mappings"), List.class);
                if (mappingExpressions != null) {
                    for (String mappingExpression : mappingExpressions) {
                        Object eval = ReflectionUtils.evaluate(mappingExpression);
                        if (eval instanceof Mapping) {
                            plugin.addMapping((Mapping) eval);
                        } else {
                            Collection<?> mappings = (Collection<?>) eval;
                            for (Object mapping : mappings) {
                                plugin.addMapping((Mapping) mapping);
                            }
                        }
                    }
                }

                // ChannelConversions.
                List<String> conversionExpressions = asInstanceOf(values.get("conversions"), List.class);
                if (conversionExpressions != null) {
                    for (String conversionExpression : conversionExpressions) {
                        Object eval = ReflectionUtils.evaluate(conversionExpression);
                        if (eval instanceof ChannelConversion) {
                            plugin.addChannelConversion((ChannelConversion) eval);
                        } else {
                            Collection<?> conversions = (Collection<?>) eval;
                            for (Object conversion : conversions) {
                                plugin.addChannelConversion((ChannelConversion) conversion);
                            }
                        }
                    }
                }

                // Properties.
                Map<String, Object> properties = asInstanceOf(values.get("properties"), Map.class);
                if (properties != null) {
                    properties.forEach(plugin::addProperty);
                }
            }

            return plugin;
        } catch (Exception e) {
            throw new RheemException(String.format("Configuration file %s seems to be corrupt.", yamlUrl), e);
        }
    }

    /**
     * Checks and casts the given {@link Object}.
     *
     * @param o   to be casted
     * @param t   to that should be casted
     * @param <T>
     * @return {@code o}, casted
     */
    @SuppressWarnings("unchecked")
    public static <T> T asInstanceOf(Object o, Class<? super T> t) {
        if (o == null) return null;
        Validate.isInstanceOf(t, o, "Expected %s to be of type %s (is %s).", o, t, o == null ? null : o.getClass());
        return (T) o;
    }

    public void addRequiredPlatform(Platform platform) {
        this.requiredPlatforms.add(platform);
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return this.requiredPlatforms;
    }

    public void addMapping(Mapping mapping) {
        this.mappings.add(mapping);
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    public void addChannelConversion(ChannelConversion channelConversion) {
        this.channelConversions.add(channelConversion);
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return this.channelConversions;
    }

    public void addProperty(String key, Object value) {
        if (value == null) {
            this.properties.remove(key);
        } else {
            this.properties.put(key, Objects.toString(value));
        }
    }

    @Override
    public void setProperties(Configuration configuration) {
        properties.forEach(configuration::setProperty);
    }
}
