package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;

import java.util.Collection;

/**
 * Mapping from {@link DoWhileOperator} to {@link FlinkDoWhileOperator}.
 */
@SuppressWarnings("unchecked")
public class DoWhileMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return null;
    }
}
