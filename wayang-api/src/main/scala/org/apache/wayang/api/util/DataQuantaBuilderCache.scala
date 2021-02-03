package org.apache.wayang.api.util

import org.apache.wayang.api.DataQuanta

/**
  * Caches products of [[org.apache.wayang.api.DataQuantaBuilder]]s that need to be executed at once, e.g., because they
  * belong to different [[org.apache.wayang.core.plan.wayangplan.OutputSlot]]s of the same custom [[org.apache.wayang.core.plan.wayangplan.Operator]].
  */
class DataQuantaBuilderCache {

  private var _cache: IndexedSeq[DataQuanta[_]] = _

  /**
    * Tell whether there is something in this cache.
    *
    * @return whether the cache has been filled
    */
  def hasCached = _cache != null

  /**
    * Get previously cached [[DataQuanta]].
    *
    * @param index index of the [[DataQuanta]] as they were passed in [[cache()]]
    * @tparam T the requested type of [[DataQuanta]]
    * @return the cached [[DataQuanta]]
    */
  def apply[T](index: Int) = {
    assert(hasCached)
    _cache(index).asInstanceOf[DataQuanta[T]]
  }

  /**
    * Cache [[DataQuanta]]. Should only be called once.
    *
    * @param dataQuanta the [[DataQuanta]] that should be cached
    */
  def cache(dataQuanta: Iterable[DataQuanta[_]]) = {
    assert(!hasCached)
    _cache = dataQuanta.toIndexedSeq
  }

}
