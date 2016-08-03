package org.qcri.rheem.postgres.mapping;

import org.qcri.rheem.core.mapping.Mapping;

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
