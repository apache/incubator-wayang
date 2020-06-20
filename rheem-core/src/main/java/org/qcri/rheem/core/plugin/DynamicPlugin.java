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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This {@link Plugin} can be arbitrarily customized.
 */
public class DynamicPlugin implements Plugin {

    private final Set<Platform> requiredPlatforms = new HashSet<>(), excludedRequiredPlatforms = new HashSet<>();

    private final Set<Mapping> mappings = new HashSet<>(), excludedMappings = new HashSet<>();

    private final Set<ChannelConversion> channelConversions = new HashSet<>(), excludedChannelConversions = new HashSet<>();

    private final Map<String, String> properties = new HashMap<>();

    /**
     * Loads a YAML file of the following format:
     * <pre>{@code
     * # mappings to be loaded by this plugin
     * mappings:
     *   include:
     *     - <expression that evaluates to a Mapping instance or Collection of such>
     *     - ...
     *   exclude:
     *     - <expression that evaluates to a Mapping instance or Collection of such>
     *     - ...
     *
     * # channel conversions to be loaded by this plugin
     * conversions:
     *   include:
     *     - <expression that evaulates to a ChannelConversion instance or Collection of such>
     *     - ..
     *   exclude:
     *     - <expression that evaulates to a ChannelConversion instance or Collection of such>
     *     - ..
     *
     * # properties configured by this plugin
     * properties:
     *   <property name>: <property value>
     *
     * # required platforms
     * platforms:
     *   include:
     *     - <expression that evaluates to a Platform instance or Collection of such>
     *     - ..
     *   exclude:
     *     - <expression that evaluates to a Platform instance or Collection of such>
     *     - ..
     *
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
            DynamicPlugin.<Map<String, Object>>ifPresent(yaml, Map.class, values -> {
                // Platforms.
                DynamicPlugin.<Map<String, Object>>ifPresent(values.get("platforms"), Map.class, platforms -> {
                    DynamicPlugin.<List<String>>ifPresent(platforms.get("include"), List.class, expressions -> {
                        for (String expression : expressions) {
                            Object eval = ReflectionUtils.evaluate(expression);
                            if (eval instanceof Platform) {
                                plugin.addRequiredPlatform((Platform) eval);
                            } else {
                                Collection<?> platformCollection = (Collection<?>) eval;
                                for (Object platform : platformCollection) {
                                    plugin.addRequiredPlatform((Platform) platform);
                                }
                            }
                        }
                    });
                    DynamicPlugin.<List<String>>ifPresent(platforms.get("exclude"), List.class, expressions -> {
                        for (String expression : expressions) {
                            Object eval = ReflectionUtils.evaluate(expression);
                            if (eval instanceof Platform) {
                                plugin.excludeRequiredPlatform((Platform) eval);
                            } else {
                                Collection<?> platformCollection = (Collection<?>) eval;
                                for (Object platform : platformCollection) {
                                    plugin.excludeRequiredPlatform((Platform) platform);
                                }
                            }
                        }
                    });
                });

                // Mappings.
                DynamicPlugin.<Map<String, Object>>ifPresent(values.get("mappings"), Map.class, mappings -> {
                    DynamicPlugin.<List<String>>ifPresent(mappings.get("include"), List.class, expressions -> {
                        for (String expression : expressions) {
                            Object eval = ReflectionUtils.evaluate(expression);
                            if (eval instanceof Mapping) {
                                plugin.addMapping((Mapping) eval);
                            } else {
                                Collection<?> collection = (Collection<?>) eval;
                                for (Object element : collection) {
                                    plugin.addMapping((Mapping) element);
                                }
                            }
                        }
                    });
                    DynamicPlugin.<List<String>>ifPresent(mappings.get("exclude"), List.class, expressions -> {
                        for (String expression : expressions) {
                            Object eval = ReflectionUtils.evaluate(expression);
                            if (eval instanceof Mapping) {
                                plugin.excludeMapping((Mapping) eval);
                            } else {
                                Collection<?> collection = (Collection<?>) eval;
                                for (Object element : collection) {
                                    plugin.excludeMapping((Mapping) element);
                                }
                            }
                        }
                    });
                });

                // ChannelConversions.
                DynamicPlugin.<Map<String, Object>>ifPresent(values.get("conversions"), Map.class, conversions -> {
                    DynamicPlugin.<List<String>>ifPresent(conversions.get("include"), List.class, expressions -> {
                        for (String expression : expressions) {
                            Object eval = ReflectionUtils.evaluate(expression);
                            if (eval instanceof ChannelConversion) {
                                plugin.addChannelConversion((ChannelConversion) eval);
                            } else {
                                Collection<?> collection = (Collection<?>) eval;
                                for (Object element : collection) {
                                    plugin.addChannelConversion((ChannelConversion) element);
                                }
                            }
                        }
                    });
                    DynamicPlugin.<List<String>>ifPresent(conversions.get("exclude"), List.class, expressions -> {
                        for (String expression : expressions) {
                            Object eval = ReflectionUtils.evaluate(expression);
                            if (eval instanceof ChannelConversion) {
                                plugin.excludeChannelConversion((ChannelConversion) eval);
                            } else {
                                Collection<?> collection = (Collection<?>) eval;
                                for (Object element : collection) {
                                    plugin.excludeChannelConversion((ChannelConversion) element);
                                }
                            }
                        }
                    });
                });

                // Properties.
                DynamicPlugin.<Map<String, Object>>ifPresent(values.get("properties"), Map.class, properties -> {
                    properties.forEach(plugin::addProperty);
                });
            });

            return plugin;
        } catch (Exception e) {
            throw new RheemException(String.format("Configuration file %s seems to be corrupt.", yamlUrl), e);
        }
    }


    /**
     * Checks and casts the given {@link Object}. If it is not {@code null}, feed it to a {@link Consumer}.
     *
     * @param o        to be casted
     * @param t        to that should be casted
     * @param consumer accepts the casted {@code o} unless it is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static <T> void ifPresent(Object o, Class<? super T> t, Consumer<T> consumer) {
        if (o == null) return;
        Validate.isInstanceOf(t, o, "Expected %s to be of type %s (is %s).", o, t, o.getClass());
        consumer.accept((T) o);
    }

    public void addRequiredPlatform(Platform platform) {
        this.requiredPlatforms.add(platform);
        this.excludedRequiredPlatforms.remove(platform);
    }

    public void excludeRequiredPlatform(Platform platform) {
        this.excludedRequiredPlatforms.add(platform);
        this.requiredPlatforms.remove(platform);
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return this.requiredPlatforms;
    }

    @Override
    public Collection<Platform> getExcludedRequiredPlatforms() {
        return this.excludedRequiredPlatforms;
    }

    public void addMapping(Mapping mapping) {
        this.mappings.add(mapping);
        this.excludedMappings.remove(mapping);
    }

    public void excludeMapping(Mapping mapping) {
        this.excludedMappings.add(mapping);
        this.mappings.remove(mapping);
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public Collection<Mapping> getExcludedMappings() {
        return this.excludedMappings;
    }

    public void addChannelConversion(ChannelConversion channelConversion) {
        this.channelConversions.add(channelConversion);
        this.excludedChannelConversions.remove(channelConversion);
    }

    public void excludeChannelConversion(ChannelConversion channelConversion) {
        this.excludedChannelConversions.add(channelConversion);
        this.channelConversions.remove(channelConversion);
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return this.channelConversions;
    }

    @Override
    public Collection<ChannelConversion> getExcludedChannelConversions() {
        return this.excludedChannelConversions;
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
