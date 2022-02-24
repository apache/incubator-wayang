/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.wayang.api.sql.executor;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Custom schemaFacoty entry
 * Configure factory class from configuration file
 * ModelHandler will tune this factory class
 * */
public class CustomSchemaFactory implements SchemaFactory {

  /**
   * parentSchema his parent node, usually root
   * name the name of the database, which is defined in the model
   * operand is also defined in mode, is a Map type, used to pass in custom parameters.
   * */
  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    return new CustomSchema();
  }

}
