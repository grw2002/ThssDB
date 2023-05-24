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
package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.plan.impl.SelectFromTablePlan;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;

import java.util.ArrayList;
import java.util.List;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  private Manager manager;

  public ThssDBSQLVisitor(Manager manager) {
    this.manager = manager;
  }

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) throws RuntimeException {
    List<MetaInfo> metaInfos = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    List<QueryTable> queryTables = new ArrayList<>();
    Database currentDB = manager.getCurrentDatabase();
    if (currentDB == null) {
      throw new DatabaseNotExistException();
    }
    for (SQLParser.TableQueryContext tableQueryContext : ctx.tableQuery()) {
      String tableName = tableQueryContext.getText();
      Table table = currentDB.findTableByName(tableName);
      queryTables.add(new QueryTable(table));
      if (table == null) {
        throw new TableNotExistException();
      }
      tables.add(table);
      metaInfos.add(new MetaInfo(tableName, new ArrayList<>()));
    }
    for (SQLParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      SQLParser.ColumnFullNameContext columnFullNameContext = resultColumnContext.columnFullName();
      String columnName = columnFullNameContext.columnName().getText();
      if (columnFullNameContext.tableName() == null) {
        for (int i = 0; i < tables.size(); i++) {
          Table table = tables.get(i);
          Column column = table.findColumnByName(columnName);
          if (column == null) {
            continue;
          }
          metaInfos.get(i).getColumns().add(column);
          // TODO
          break;
        }
      } else {
        String tableName = columnFullNameContext.tableName().getText();
        for (int i = 0; i < tables.size(); i++) {
          Table table = tables.get(i);
          if (table.tableName == tableName) {
            Column column = table.findColumnByName(columnName);
            if (column == null) {
              // TODO
              throw new TableNotExistException();
            }
            metaInfos.get(i).getColumns().add(column);
            break;
          }
        }
      }
    }
    return new SelectFromTablePlan(
        new QueryResult((QueryTable[]) queryTables.toArray(), metaInfos));
  }
  // TODO: parser to more logical plan
}
