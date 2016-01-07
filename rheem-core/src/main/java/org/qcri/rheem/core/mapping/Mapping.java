package org.qcri.rheem.core.mapping;

import java.util.Collection;

/**
 * A mapping issues a set of {@link PlanTransformation}s that make up the complete mapping.
 */
public interface Mapping {

    /**
     * @return the transformations that make up this mapping
     */
    Collection<PlanTransformation> getTransformations();

}
