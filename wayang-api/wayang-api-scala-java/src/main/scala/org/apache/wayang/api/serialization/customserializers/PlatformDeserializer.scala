/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
