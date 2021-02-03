package org.apache.wayang.postgres.mapping;

import org.apache.wayang.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for the {@link Mapping}s supported for this platform.
 */
public class Mappings {

    public static final Collection<Mapping> ALL = Arrays.asList(
            new FilterMapping(),
            new ProjectionMapping()
    );

}
