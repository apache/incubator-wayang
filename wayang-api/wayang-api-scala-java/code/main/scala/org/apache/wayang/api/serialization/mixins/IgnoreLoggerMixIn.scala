package org.apache.wayang.api.serialization.mixins

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.logging.log4j.Logger

abstract class IgnoreLoggerMixIn {
  @JsonIgnore
  private var logger: Logger = _
}
