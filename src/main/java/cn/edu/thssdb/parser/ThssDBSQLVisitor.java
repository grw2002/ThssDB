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

import cn.edu.thssdb.exception.ColumnNotExistException;
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
  public LogicalPlan visitShowTablesStmt(SQLParser.ShowTablesStmtContext ctx) {
    if (!ctx.databaseName().isEmpty()) {
      String databaseName = ctx.databaseName().getText();
      return new ShowTablesPlan(databaseName);
    }
    return new ShowTablesPlan(manager.getCurrentDatabaseName());
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

  @Override
  public LogicalPlan visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    String tableName = ctx.tableName().getText();
    String columnName = ctx.columnName().getText();
    String newValue = ctx.expression().getText();
    List<String> conditions = new ArrayList<>();

    if (newValue.startsWith("'") && newValue.endsWith("'")) {
      newValue = newValue.substring(1, newValue.length() - 1);
    }
    if (ctx.multipleCondition() != null) {
      SQLParser.MultipleConditionContext multipleConditionCtx = ctx.multipleCondition();
      // 遍历条件树
      traverseConditionTree(multipleConditionCtx, conditions);
    }
    // 创建UpdatePlan，传递表名，列名，新值和条件
    return new UpdatePlan(tableName, columnName, newValue, conditions);
  }

  public LogicalPlan visitSelectStmt2(SQLParser.SelectStmtContext ctx) {
    return new SelectPlan(new ArrayList<>(), new ArrayList<>(), "", "");
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) throws RuntimeException {
    List<MetaInfo> metaInfos = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    List<QueryTable> queryTables = new ArrayList<>();
    String joinCondition = null;
    String whereCondition = null;

    Database currentDB = manager.getCurrentDatabase();
    if (currentDB == null) {
      throw new DatabaseNotExistException();
    }

    for (SQLParser.TableQueryContext tableQueryContext : ctx.tableQuery()) {
      String tableName = tableQueryContext.tableName(0).getText();
      Table table = currentDB.findTableByName(tableName);
      if (table == null) {
        throw new TableNotExistException(tableName);
      }
      queryTables.add(new QueryTable(table));
      tables.add(table);
      metaInfos.add(new MetaInfo(tableName, new ArrayList<>()));

      // Check for JOIN operation
      if (tableQueryContext.tableName(1) != null) {
        // Add tableName for JOIN
        tableName = tableQueryContext.tableName(1).getText();
        table = currentDB.findTableByName(tableName);
        if (table == null) {
          throw new TableNotExistException(tableName);
        }
        queryTables.add(new QueryTable(table));
        tables.add(table);
        metaInfos.add(new MetaInfo(tableName, new ArrayList<>()));

        // Get JOIN condition
        List<String> joinConditions = new ArrayList<>();
        traverseConditionTree(tableQueryContext.multipleCondition(), joinConditions);
        joinCondition = String.join("", joinConditions);
      }
    }

    for (SQLParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      SQLParser.ColumnFullNameContext columnFullNameContext = resultColumnContext.columnFullName();
      if (resultColumnContext.MUL() != null) { // Check for the '*' symbol
        for (MetaInfo metaInfo : metaInfos) {
          Table table = currentDB.findTableByName(metaInfo.getTableName());
          if (table == null) {
            throw new TableNotExistException(metaInfo.getTableName());
          }
          metaInfo.getColumns().addAll(table.getColumns());
        }
        continue;
      }
      String columnName = columnFullNameContext.columnName().getText();
      if (columnFullNameContext.tableName() == null) {
        for (int i = 0; i < tables.size(); i++) {
          Table tableIter = tables.get(i);
          Column column = tableIter.findColumnByName(columnName);
          if (column == null) {
            continue;
          }
          metaInfos.get(i).getColumns().add(column);
          break;
        }
      } else {
        String tableSpecifiedName = columnFullNameContext.tableName().getText();
        for (int i = 0; i < tables.size(); i++) {
          Table tableIter = tables.get(i);
          if (tableIter.tableName.equals(tableSpecifiedName)) {
            Column column = tableIter.findColumnByName(columnName);
            if (column == null) {
              throw new ColumnNotExistException(columnName);
            }
            metaInfos.get(i).getColumns().add(column);
            break;
          }
        }
      }
    }

    if (ctx.K_WHERE() != null) {
      List<String> whereConditions = new ArrayList<>();
      traverseConditionTree(ctx.multipleCondition(), whereConditions);
      whereCondition = String.join("", whereConditions);
    }

    // Construct SelectPlan
    SelectPlan selectPlan = new SelectPlan(queryTables, metaInfos, joinCondition, whereCondition);

    return selectPlan;
  }

  // TODO: parser to more logical plan
}
