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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.net.URL;
import java.util.Map;

/**
 * Similar to the database, Schema represents the database
 * */

public class CustomSchema extends AbstractSchema {
  private Map<String, Table> tableMap;

  @Override
  protected Map<String, Table> getTableMap() {
    URL url = CustomSchema.class.getResource("/data.csv");
    Source source = Sources.of(url);
    if (tableMap == null) {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
      // A database has multiple table names, initialized here, the case should be noted, TEST01 is the table name.
      builder.put("TEST01", new CustomTable(source));
      tableMap = builder.build();
    }
    return tableMap;
  }

}
