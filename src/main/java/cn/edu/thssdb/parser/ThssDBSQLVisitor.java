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
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.plan.impl.DropDatabasePlan;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;

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

  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new DropDatabasePlan(ctx.databaseName().getText());
  }

  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    List<Column> columns = new ArrayList<>();
    for (SQLParser.ColumnDefContext columnDefContext : ctx.columnDef()) {
      String columnName = columnDefContext.columnName().getText();
      String columnType = columnDefContext.typeName().getText();
      int primary = 0;
      boolean notnull = false;
      for (SQLParser.ColumnConstraintContext columnConstraintContext :
          columnDefContext.columnConstraint()) {
        //         judge whether it's primary key
        if (columnConstraintContext.K_PRIMARY() != null
            && columnConstraintContext.K_KEY() != null) {
          primary = 1;
        } else if (columnConstraintContext.K_NOT() != null
            && columnConstraintContext.K_NULL() != null) {
          notnull = true;
        }
      }
      ColumnType columnTypeEnum = ColumnType.valueOf(columnType.toUpperCase());
      columns.add(new Column(columnName, columnTypeEnum, primary, notnull, 128, tableName));
    }
    return new CreateTablePlan(tableName, columns);
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTablePlan(ctx.tableName().getText());
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
    return new SelectPlan(queryTables, metaInfos);
  }
  // TODO: parser to more logical plan
}
