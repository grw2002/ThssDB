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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;

public class AlterTablePlan extends LogicalPlan {
  public enum Operation {
    ADD_COLUMN,
    DROP_COLUMN,
    ALTER_COLUMN,
    RENAME_COLUMN,
    ADD_CONSTRAINT,
    DROP_CONSTRAINT
  }

  private String tableName;
  private Operation operation;
  private Column column;
  private String newColumnName; // 用于RENAME COLUMN操作
  private String newColumnType; // 用于ALTER COLUMN操作
  // private TableConstraint constraint;

  // ... getters and setters ...
  public AlterTablePlan() {
    super(LogicalPlanType.ALTER_TABLE);
  }

  public AlterTablePlan(
      String tableName,
      Operation operation,
      Column column,
      String newColumnType,
      String newColumnName) {
    super(LogicalPlanType.ALTER_TABLE);
    this.tableName = tableName;
    this.operation = operation;
    this.column = column;
    this.newColumnName = newColumnName;
    this.newColumnType = newColumnType;
  }

  public String getTableName() {
    return tableName;
  }

  public Operation getOperation() {
    return operation;
  }

  public Column getColumn() {
    return column;
  }

  public String getColumnName() {
    return column.getName();
  }

  public String getNewColumnType() {
    return newColumnType;
  }

  public String getNewColumnName() {
    return newColumnName;
  }
}
