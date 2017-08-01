package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;

import java.util.Collection;

/**
 * Mapping from {@link LoopOperator} to {@link FlinkLoopOperator}.
 */
@SuppressWarnings("unchecked")
public class LoopMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return null;
    }
}
