package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;

import java.util.Collection;

/**
 * Mapping from {@link RepeatOperator} to {@link FlinkRepeatOperator}.
 */
@SuppressWarnings("unchecked")
public class RepeatMapping implements Mapping{
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return null;
    }
}
