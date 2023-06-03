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

import java.util.*;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  private Manager manager;

  public ThssDBSQLVisitor(Manager manager) {
    this.manager = manager;
  }

  /*
  utils
  */
  public static Map<String, Object> parseColumnType(String columnType) {
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

    Map<String, Object> result = new HashMap<>();
    result.put("columnTypeEnum", columnTypeEnum);
    result.put("stringLength", stringLength);

    return result;
  }

  /*
   utils begin
  */

  private Column parseColumnDef(SQLParser.ColumnDefContext ctx) {
    String columnName = ctx.columnName().getText();
    String columnType = ctx.typeName().getText();
    int primary = 0;
    boolean notnull = false;

    for (SQLParser.ColumnConstraintContext columnConstraintContext : ctx.columnConstraint()) {
      if (columnConstraintContext.K_NOT() != null && columnConstraintContext.K_NULL() != null) {
        notnull = true;
      } else if (columnConstraintContext.K_PRIMARY() != null
          && columnConstraintContext.K_KEY() != null) {
        primary = 1;
      }
    }

    ColumnType columnTypeEnum;
    int stringLength = 128; // 默认字符串长度

    Map<String, Object> result = parseColumnType(columnType);
    columnTypeEnum = (ColumnType) result.get("columnTypeEnum");
    stringLength = (int) result.get("stringLength");

    return new Column(columnName, columnTypeEnum, primary, notnull, stringLength);
  }

  /*
   utils end
  */
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

      Map<String, Object> result = parseColumnType(columnType);
      columnTypeEnum = (ColumnType) result.get("columnTypeEnum");
      stringLength = (int) result.get("stringLength");

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
  public LogicalPlan visitAlterTableStmt(SQLParser.AlterTableStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    Column column = null;
    String newColumnName = null;
    String newColumnType = null;

    // columnType.INT -> default, has no meaning

    if (ctx.K_ADD() != null) {
      if (ctx.columnDef() != null) {
        // 添加列
        column = parseColumnDef(ctx.columnDef());

        // 检查是否存在 NOT NULL 以及 PRIMARY 约束
        if (column.getNotNull() || column.getPrimary() != 0) {
          throw new UnsupportedOperationException(
              "Adding a column with NOT NULL constraint is not allowed if the table is not empty.");
        }

        return new AlterTablePlan(
            tableName, AlterTablePlan.Operation.ADD_COLUMN, column, null, null);
      } else if (ctx.tableConstraint() != null) {
        // 添加约束
        // TODO
        return new AlterTablePlan(
            tableName, AlterTablePlan.Operation.ADD_CONSTRAINT, column, null, null);
      }
    } else if (ctx.K_DROP() != null) {
      if (ctx.columnName() != null) {
        // 删除列
        String columnName = ctx.columnName(0).getText();
        column = new Column(columnName, ColumnType.INT, 0, false, 128);
        return new AlterTablePlan(
            tableName, AlterTablePlan.Operation.DROP_COLUMN, column, null, null);
      } else if (ctx.tableConstraint() != null) {
        // 删除约束
        // TODO
        return new AlterTablePlan(
            tableName, AlterTablePlan.Operation.DROP_CONSTRAINT, column, null, null);
      }
    } else if (ctx.K_ALTER() != null) {
      String columnName = ctx.columnName(0).getText();
      newColumnType = ctx.typeName().getText();
      column = new Column(columnName, ColumnType.INT, 0, false, 128);
      return new AlterTablePlan(
          tableName, AlterTablePlan.Operation.ALTER_COLUMN, column, newColumnType, null);
    } else if (ctx.K_RENAME() != null) {
      String columnName = ctx.columnName(0).getText();
      newColumnName = ctx.columnName(1).getText();
      column = new Column(columnName, ColumnType.INT, 0, false, 128);
      return new AlterTablePlan(
          tableName, AlterTablePlan.Operation.RENAME_COLUMN, column, null, newColumnName);
    }

    return null;
  }

  @Override
  public LogicalPlan visitShowRowsStmt(SQLParser.ShowRowsStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    return new ShowRowsPlan(tableName);
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    String tableName = ctx.tableName().getText();

    // column names
    List<String> columnNames = new ArrayList<>();
    for (SQLParser.ColumnNameContext columnNameCtx : ctx.columnName()) {
      columnNames.add(columnNameCtx.getText());
    }

    // values
    List<List<String>> values = new ArrayList<>();
    for (SQLParser.ValueEntryContext valueEntryCtx : ctx.valueEntry()) {
      List<String> value = new ArrayList<>();
      for (SQLParser.LiteralValueContext literalValueCtx : valueEntryCtx.literalValue()) {
        value.add(literalValueCtx.getText());
      }
      values.add(value);
    }
    return new InsertPlan(tableName, columnNames, values);
  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    List<String> conditions = new ArrayList<>();
    if (ctx.multipleCondition() != null) {
      SQLParser.MultipleConditionContext multipleConditionCtx = ctx.multipleCondition();
      // 遍历条件树
      traverseConditionTree(multipleConditionCtx, conditions);
    }

    return new DeletePlan(tableName, conditions);
  }

  // 递归遍历条件树，获取所有的条件和逻辑操作符
  private void traverseConditionTree(
      SQLParser.MultipleConditionContext ctx, List<String> conditions) {
    if (ctx.condition() != null) {
      SQLParser.ConditionContext conditionCtx = ctx.condition();
      String leftExpr = conditionCtx.expression(0).getText();
      String comparator = conditionCtx.comparator().getText();
      String rightExpr = conditionCtx.expression(1).getText();
      conditions.add(leftExpr + " " + comparator + " " + rightExpr);
    } else {
      if (ctx.multipleCondition(0) != null) {
        traverseConditionTree(ctx.multipleCondition(0), conditions);
      }
      // 添加逻辑操作符
      if (ctx.AND() != null) {
        conditions.add("AND");
      } else if (ctx.OR() != null) {
        conditions.add("OR");
      }
      if (ctx.multipleCondition(1) != null) {
        traverseConditionTree(ctx.multipleCondition(1), conditions);
      }
    }
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
      if (table == null) {
        throw new TableNotExistException();
      }
      queryTables.add(new QueryTable(table));
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
