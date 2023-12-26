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


package org.apache.wayang.api.async

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Represents an asynchronous result of data quanta processing.
 *
 * The `DataQuantaAsyncResult2` class is a case class used to encapsulate the result of an asynchronous data quanta processing.
 * It contains the temporary output file path, the class tag of the output type, and the future representing the completion
 * of the processing.
 *
 * @param tempFileOut The temporary output file path.
 * @param classTag    The class tag of the output type.
 * @param future      The future representing the completion of the processing.
 * @tparam Out The type of the output.
 */
case class DataQuantaAsyncResult[Out: ClassTag](tempFileOut: String, classTag: ClassTag[Out], future: Future[_])
