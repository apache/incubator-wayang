package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.optimizer.costs.EstimationContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a property of an {@link Operator} that is relevant to the estimation process, i.e., should be provided
 * in {@link EstimationContext}s.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface EstimationContextProperty {
}
