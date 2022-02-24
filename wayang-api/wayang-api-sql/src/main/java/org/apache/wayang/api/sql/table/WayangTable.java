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

package org.apache.wayang.api.sql.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class WayangTable extends AbstractTable {

  private final String filePath;
  private final String[] fieldNames;

  /** Creates a PigTable. */
  public WayangTable(String filePath, String[] fieldNames) {
    this.filePath = filePath;
    this.fieldNames = fieldNames;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (String fieldName : fieldNames) {
      // only supports CHARARRAY types for now
      final RelDataType relDataType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

      final RelDataType nullableRelDataType = typeFactory
          .createTypeWithNullability(relDataType, true);
      builder.add(fieldName, nullableRelDataType);
    }
    return builder.build();
  }

  public String getFilePath() {
    return filePath;
  }

}
