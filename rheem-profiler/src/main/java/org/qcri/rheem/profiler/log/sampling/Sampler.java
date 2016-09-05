package org.qcri.rheem.profiler.log.sampling;

import java.util.Collection;
import java.util.List;

/**
 * Interface that describes a sampling algorithm that can give bias towards certain elements..
 *
 * @param <T> the type of sampled elements
 */
public interface Sampler<T> {

    /**
     * Create a sample from a given {@link Collection}.
     *
     * @param set    the set of elements to be sampled from
     * @param battle lets two elements compete
     * @return the sample; may contain {@code null}s dependent on the implementation
     */
    List<T> sample(Collection<T> set, Battle<T> battle, double selectionProbability);

}
