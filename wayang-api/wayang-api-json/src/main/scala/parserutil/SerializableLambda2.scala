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
package org.apache.wayang.api.json.parserutil

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class SerializableLambda2[A, B, C](string: String) extends ((A, B) => C) with Serializable {
  @transient lazy val lambdaFunction: (A, B) => C = {
    val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
    try {
      val tree = toolbox.parse(string)
      toolbox.compile(tree)().asInstanceOf[(A, B) => C]
    }
    catch {
      case e: Throwable =>
        ParsingErrors.handleException(e, string)
    }
  }

  def apply(arg1: A, arg2: B): C = lambdaFunction(arg1, arg2)
}

object SerializableLambda2 {
  def createLambda[A, B, C](string: String): SerializableLambda2[A, B, C] = {
    new SerializableLambda2[A, B, C](string)
  }
}

