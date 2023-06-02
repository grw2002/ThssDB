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
    ADD_CONSTRAINT,
    DROP_CONSTRAINT
  }

  private String tableName;
  private Operation operation;
  private Column column;
  // private String columnName;
  // private TableConstraint constraint;

  // ... getters and setters ...
  public AlterTablePlan() {
    super(LogicalPlanType.ALTER_TABLE);
  }

  public AlterTablePlan(String tableName, Operation operation, Column column) {
    super(LogicalPlanType.ALTER_TABLE);
    this.tableName = tableName;
    this.operation = operation;
    this.column = column;
  }

  public String getTableName() {
    return this.tableName;
  }

  public Operation getOperation() {
    return this.operation;
  }

  public Column getColumn() {
    return this.column;
  }

  public String getColumnName() {
    return this.column.getName();
  }
}
