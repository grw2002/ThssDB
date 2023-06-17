package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.QueryResult2;
import cn.edu.thssdb.query.QueryTable2;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  private Manager manager;

  public IServiceHandler() {
    super();
    manager = Manager.getInstance();
  }

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    manager.persist("metadata.meta");

    return new DisconnectResp(StatusUtil.success());
  }

  //  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }

    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement, manager);
    System.out.println("[DEBUG] " + plan);
    switch (plan.getType()) {
      case CREATE_DB:
        try {
          manager.createDatabase(((CreateDatabasePlan) plan).getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DROP_DB:
        try {
          System.out.println("[DEBUG] " + plan);
          manager.deleteDatabase(((DropDatabasePlan) plan).getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case USE_DB:
        try {
          System.out.println("[DEBUG] " + plan);
          manager.switchDatabase(((UseDatabasePlan) plan).getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SHOW_DB:
        try {
          System.out.println("[DEBUG] " + plan);
          ShowDatabasesPlan showDatabasesPlan = (ShowDatabasesPlan) plan;
          List<String> databases = manager.showDatabases();
          String currentDbName = manager.getCurrentDatabaseName();
          ExecuteStatementResp showDatabaseResp =
              new ExecuteStatementResp(StatusUtil.success(), true);
          showDatabaseResp.columnsList = Arrays.asList("Databases", "Status");
          showDatabaseResp.rowList =
              databases.stream()
                  .map(
                      dbName ->
                          Arrays.asList(
                              dbName, dbName.equals(currentDbName) ? "IN USE" : "NOT IN USE"))
                  .collect(Collectors.toList());
          return showDatabaseResp;
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }

      case SHOW_TABLES:
        try {
          System.out.println("[DEBUG] " + plan);
          ShowTablesPlan showTablesPlan = (ShowTablesPlan) plan;
          String currentDbName = showTablesPlan.getCurrentDatabase();

          List<String> tables = manager.getTablesName();
          ExecuteStatementResp showTablesResp =
              new ExecuteStatementResp(StatusUtil.success(), true);
          showTablesResp.columnsList = Collections.singletonList("Table");
          showTablesResp.rowList = tables.stream().map(Arrays::asList).collect(Collectors.toList());
          return showTablesResp;
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case CREATE_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          CreateTablePlan createTablePlan = (CreateTablePlan) plan;
          manager.createTable(createTablePlan.getTableName(), createTablePlan.getColumns());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DROP_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          DropTablePlan dropTablePlan = (DropTablePlan) plan;
          manager.dropTable(dropTablePlan.getTableName(), dropTablePlan.isIfExists());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SHOW_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          ShowTablePlan showTablePlan = (ShowTablePlan) plan;
          List<Column> columns = manager.showTable(showTablePlan.getTableName());
          ExecuteStatementResp showTableResp = new ExecuteStatementResp(StatusUtil.success(), true);
          showTableResp.columnsList =
              Arrays.asList("Columns", "Type", "Primary", "Not Null", "Max Length(String)");
          showTableResp.rowList =
              columns.stream()
                  .map(
                      column ->
                          Arrays.asList(
                              column.getName(),
                              column.getType().toString(),
                              (column.isPrimary()) ? "PRIMARY KEY" : "NOT PRIMARY",
                              (column.isNotNull()) ? "NOT NULL" : "CAN NULL",
                              (column.getType() == ColumnType.STRING)
                                  ? String.valueOf(column.getMaxLength())
                                  : "None"))
                  .collect(Collectors.toList());
          return showTableResp;
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case ALTER_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          AlterTablePlan alterTablePlan = (AlterTablePlan) plan;
          switch (alterTablePlan.getOperation()) {
            case ADD_COLUMN:
              manager.addColumn(alterTablePlan.getTableName(), alterTablePlan.getColumn());
              break;
            case DROP_COLUMN:
              manager.dropColumn(alterTablePlan.getTableName(), alterTablePlan.getColumnName());
              break;
            case ALTER_COLUMN:
              manager.alterColumnType(
                  alterTablePlan.getTableName(),
                  alterTablePlan.getColumnName(),
                  alterTablePlan.getNewColumnType());
              break;
            case RENAME_COLUMN:
              manager.renameColumn(
                  alterTablePlan.getTableName(),
                  alterTablePlan.getColumnName(),
                  alterTablePlan.getNewColumnName());
              break;
            case ADD_CONSTRAINT:
              // TODO: 需要在AlterTablePlan中添加一个getConstraint方法来获取约束，然后调用添加约束的方法
              // manager.addConstraint(alterTablePlan.getTableName(),
              // alterTablePlan.getConstraint());
              break;
            case DROP_CONSTRAINT:
              // TODO: 需要在AlterTablePlan中添加一个getConstraint方法来获取约束，然后调用删除约束的方法
              // manager.dropConstraint(alterTablePlan.getTableName(),
              // alterTablePlan.getConstraint());
              break;
          }
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }

      case INSERT_INTO_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          InsertPlan insertPlan = (InsertPlan) plan;
          manager.insertIntoTable(
              insertPlan.getTableName(), insertPlan.getColumnNames(), insertPlan.getValues());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DELETE_FROM_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          DeletePlan deletePlan = (DeletePlan) plan;
          String tableName = deletePlan.getTableName();
          List<String> conditions = deletePlan.getConditions();

          manager.deleteFromTable(tableName, conditions);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case UPDATE_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          UpdatePlan updatePlan = (UpdatePlan) plan;

          manager.updateTable(
              updatePlan.getTableName(),
              updatePlan.getColumnName(),
              updatePlan.getNewValue(),
              updatePlan.getCondition());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SIMPLE_SELECT_SINGLE_TABLE:
        SimpleSinglePlan simpleSinglePlan = (SimpleSinglePlan) plan;
        QueryTable2 queryTable2 =
            manager
                .getCurrentDatabase()
                .selectSimpleSingle(
                    simpleSinglePlan.getTableName(), simpleSinglePlan.getCondition());
      case SIMPLE_SELECT_JOIN_TABLE:
        SimpleJoinPlan simpleJoinPlan = (SimpleJoinPlan) plan;
      case SELECT_FROM_TABLE:
        System.out.println("[DEBUG] " + plan);
        SelectPlan2 selectPlan = ((SelectPlan2) plan);
        QueryTable2 queryTable =
            manager
                .getCurrentDatabase()
                .select(selectPlan.getTableQuerys(), selectPlan.getMultipleCondition());
        QueryResult2 queryResult =
            QueryResult2.makeResult(queryTable, selectPlan.getResultColumns());
        ExecuteStatementResp res = new ExecuteStatementResp(StatusUtil.success(), true);
        res.columnsList = queryResult.getResultColumnNames();
        res.rowList = queryResult.getRowsList();
        return res;

      default:
        break;
    }
    return null;
  }
}
