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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  private Manager manager;

  public ThssDBSQLVisitor(Manager manager) {
    this.manager = manager;
  }

  @Override
  public LogicalPlan visitCreateUserStmt(SQLParser.CreateUserStmtContext ctx) {
    return new CreateUserPlan(ctx.userName().getText(), ctx.password().getText());
  }

  @Override
  public LogicalPlan visitDropUserStmt(SQLParser.DropUserStmtContext ctx) {
    boolean ifExists = ctx.K_IF() != null && ctx.K_EXISTS() != null;
    return new DropUserPlan(ctx.userName().getText(), ifExists);
  }

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    boolean ifExists = ctx.K_IF() != null && ctx.K_EXISTS() != null;
    return new DropDatabasePlan(ctx.databaseName().getText(), ifExists);
  }

  @Override
  public LogicalPlan visitShowDbStmt(SQLParser.ShowDbStmtContext ctx) {
    return new ShowDatabasesPlan();
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new UseDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    List<Column> columns = new ArrayList<>();
    Set<String> primaryKeys = new HashSet<>();

    if (ctx.tableConstraint() != null) {
      for (SQLParser.ColumnNameContext columnNameContext : ctx.tableConstraint().columnName()) {
        primaryKeys.add(columnNameContext.getText());
      }
    }

    for (SQLParser.ColumnDefContext columnDefContext : ctx.columnDef()) {
      String columnName = columnDefContext.columnName().getText();
      String columnType = columnDefContext.typeName().getText();
      int primary = primaryKeys.contains(columnName) ? 1 : 0;
      boolean notnull = false;

      for (SQLParser.ColumnConstraintContext columnConstraintContext :
          columnDefContext.columnConstraint()) {
        if (columnConstraintContext.K_NOT() != null && columnConstraintContext.K_NULL() != null) {
          notnull = true;
        }
      }

      ColumnType columnTypeEnum;
      int stringLength = 128; // 默认字符串长度

      if (columnType.toUpperCase().contains("STRING")) {
        columnTypeEnum = ColumnType.STRING;
        // 从 columnType 字符串中提取字符串长度
        int startIndex = columnType.indexOf("(");
        int endIndex = columnType.indexOf(")");
        if (startIndex != -1 && endIndex != -1 && startIndex < endIndex) {
          String lengthString = columnType.substring(startIndex + 1, endIndex);
          try {
            stringLength = Integer.parseInt(lengthString);
          } catch (NumberFormatException e) {
            // 处理无效的字符串长度，默认使用默认长度
            stringLength = 128;
          }
        }
      } else {
        columnTypeEnum = ColumnType.valueOf(columnType.toUpperCase());
      }

      columns.add(new Column(columnName, columnTypeEnum, primary, notnull, stringLength));
    }
    return new CreateTablePlan(tableName, columns);
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    boolean ifExists = ctx.K_IF() != null && ctx.K_EXISTS() != null;
    return new DropTablePlan(tableName, ifExists);
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    return new ShowTablePlan(tableName);
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
      //      System.out.println("add table " + table.tableName);
      metaInfos.add(new MetaInfo(tableName, new ArrayList<>()));
    }
    for (SQLParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      SQLParser.ColumnFullNameContext columnFullNameContext = resultColumnContext.columnFullName();
      String columnName = columnFullNameContext.columnName().getText();
      if (columnFullNameContext.tableName() == null) {
        for (int i = 0; i < tables.size(); i++) {
          Table table = tables.get(i);
          Column column = table.findColumnByName(columnName);
          //          System.out.println("find " + table.tableName + " " + columnName + " " +
          // column);
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
          if (table.tableName.equals(tableName)) {
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
