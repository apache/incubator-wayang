package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.spark.Spark

class PlatformDeserializer extends JsonDeserializer[Platform]{

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Platform = {
    val className = p.getValueAsString

    // TODO: Add all platforms
    if (className == Java.platform().getClass.getName) {
      Java.platform()
    } else if (className == Spark.platform().getClass.getName) {
      Spark.platform()
    } else if (className == Postgres.platform().getClass.getName) {
      Postgres.platform()
    } else {
      throw new WayangException(s"Can't deserialize platform: $className")
    }
  }

}
