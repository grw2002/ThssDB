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
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.schema.Column;

import javax.swing.*;

import java.util.List;

public class CreateTablePlan extends LogicalPlan {

  private String tableName;
  private MetaInfo metaInfo;

  public CreateTablePlan(String tableName, MetaInfo metaInfo) {
    super(LogicalPlanType.CREATE_TABLE);
    this.tableName = tableName;
    this.metaInfo = metaInfo;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Column> getColumns() {
    return metaInfo.getColumns();
  }

  public int getPrimaryIndex() {
    List<Column> columns = metaInfo.getColumns();
    for (int i = 0; i < metaInfo.getColumns().size(); i++) {
      if (columns.get(i).getPrimary() == 1) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public String toString() {
    String columnInfo = "";
    for (Column column : metaInfo.getColumns()) {
      columnInfo += column.toString();
    }
    return "CreateTablePlan{" + "databaseName='" + tableName + "\', columns=" + columnInfo + '}';
  }
}
