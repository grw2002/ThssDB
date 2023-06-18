package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.TableNotExistException;
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
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  private Manager manager;
  private final String[] autoBeginStmt = {
    "insert", "delete", "update", "select", "alter", "create"
  };
  private final String[] walStmt = {
    "insert", "delete", "update", "begin", "commit", "alter", "commit;"
  };

  public IServiceHandler() {
    super();
    this.manager = Manager.getInstance();
  }

  public IServiceHandler(Manager manager) {
    super();
    this.manager = manager;
  }

  private void execAutoCommit(long session_ID) {
    manager.autoCommit(session_ID);
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

    //    System.out.println("execute" + req.getSessionId() + req.statement);
    long session_ID = req.getSessionId();
    String stmt = req.getStatement().split("\\s+")[0];
    //    System.out.println("stmt: " + stmt);
    if (Arrays.asList(autoBeginStmt).contains(stmt.toLowerCase())) {
      manager.autoBegin(session_ID);
    }

    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement, manager);
    // System.out.println("[DEBUG] " + plan);

    if (Arrays.asList(walStmt).contains(stmt.toLowerCase())
        && session_ID != Global.ADMINISTER_SESSION
        && Global.WAL_SWITCH) {
      System.out.println("Writing log: " + stmt.toLowerCase());
      manager.writeLog(req.statement, session_ID);
    }

    //    System.out.println("[DEBUG] " + plan);
    ExecuteStatementResp response;
    switch (plan.getType()) {
      case CREATE_DB:
        try {
          manager.createDatabase(((CreateDatabasePlan) plan).getDatabaseName());
          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          manager.autoCommit(session_ID);
        }
        return response;
      case DROP_DB:
        try {
          // System.out.println("[DEBUG] " + plan);
          manager.deleteDatabase(((DropDatabasePlan) plan).getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case USE_DB:
        try {
          // System.out.println("[DEBUG] " + plan);
          manager.switchDatabase(((UseDatabasePlan) plan).getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SHOW_DB:
        try {
          // System.out.println("[DEBUG] " + plan);
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
          // System.out.println("[DEBUG] " + plan);
          ShowTablesPlan showTablesPlan = (ShowTablesPlan) plan;
          String currentDbName = showTablesPlan.getCurrentDatabase();

          List<String> tables = manager.getTablesName(currentDbName);
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
          // System.out.println("[DEBUG] " + plan);
          CreateTablePlan createTablePlan = (CreateTablePlan) plan;
          manager.createTable(createTablePlan.getTableName(), createTablePlan.getColumns());
          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          manager.autoCommit(session_ID);
        }
        return response;
      case DROP_TABLE:
        try {
          // System.out.println("[DEBUG] " + plan);
          DropTablePlan dropTablePlan = (DropTablePlan) plan;
          manager.dropTable(dropTablePlan.getTableName(), dropTablePlan.isIfExists());
          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          execAutoCommit(session_ID);
        }
        return response;
      case SHOW_TABLE:
        try {
          // System.out.println("[DEBUG] " + plan);
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
          // System.out.println("[DEBUG] " + plan);
          AlterTablePlan alterTablePlan = (AlterTablePlan) plan;

          String tableName = alterTablePlan.getTableName();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          if (table == null) {
            throw new TableNotExistException(tableName);
          }
          ReentrantReadWriteLock.WriteLock lock = table.getWriteLock();
          manager.transactionAddLock(session_ID, lock);

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
          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          execAutoCommit(session_ID);
        }
        return response;

      case INSERT_INTO_TABLE:
        try {
          // System.out.println("[DEBUG] " + plan);
          InsertPlan insertPlan = (InsertPlan) plan;

          String tableName = insertPlan.getTableName();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          if (table == null) {
            throw new TableNotExistException(tableName);
          }
          ReentrantReadWriteLock.WriteLock lock = table.getWriteLock();
          manager.transactionAddLock(session_ID, lock);
          manager.insertIntoTable(
              insertPlan.getTableName(), insertPlan.getColumnNames(), insertPlan.getValues());
          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          execAutoCommit(session_ID);
        }
        return response;
      case DELETE_FROM_TABLE:
        try {
          // System.out.println("[DEBUG] " + plan);
          DeletePlan deletePlan = (DeletePlan) plan;
          String tableName = deletePlan.getTableName();
          List<String> conditions = deletePlan.getConditions();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          ReentrantReadWriteLock.WriteLock lock = table.getWriteLock();
          manager.transactionAddLock(session_ID, lock);
          manager.deleteFromTable(tableName, conditions);

          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          execAutoCommit(session_ID);
        }
        return response;
      case UPDATE_TABLE:
        try {
          // System.out.println("[DEBUG] " + plan);
          UpdatePlan updatePlan = (UpdatePlan) plan;
          String tableName = updatePlan.getTableName();
          String columnName = updatePlan.getColumnName();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);
          if (table == null) {
            throw new TableNotExistException(tableName);
          }

          ReentrantReadWriteLock.WriteLock lock = table.getWriteLock();
          manager.transactionAddLock(session_ID, lock);
          manager.updateTable(
              updatePlan.getTableName(),
              updatePlan.getColumnName(),
              updatePlan.getNewValue(),
              updatePlan.getCondition());
          response = new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          execAutoCommit(session_ID);
        }
        return response;
      case SIMPLE_SELECT_SINGLE_TABLE:
        List<Lock> locks_single_simple = new ArrayList<>();
        try {
          SimpleSinglePlan simpleSinglePlan = (SimpleSinglePlan) plan;
          Table table =
              manager.getCurrentDatabase().findTableByName(simpleSinglePlan.getTableName());
          if (table == null) {
            throw new TableNotExistException(simpleSinglePlan.getTableName());
          }
          locks_single_simple.add(table.getReadLock());
          manager.transactionAddMultipleLocks(session_ID, locks_single_simple);
          QueryTable2 queryTable =
              manager
                  .getCurrentDatabase()
                  .selectSimpleSingle(
                      simpleSinglePlan.getTableName(), simpleSinglePlan.getCondition());
          QueryResult2 queryResult =
              QueryResult2.makeResult(queryTable, simpleSinglePlan.getResultColumns());
          response = new ExecuteStatementResp(StatusUtil.success(), true);
          response.columnsList = queryResult.getResultColumnNames();
          response.rowList = queryResult.getRowsList();

        } catch (Exception e) {
          e.printStackTrace();
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          for (Lock t : locks_single_simple) {
            t.unlock();
          }
          execAutoCommit(session_ID);
        }
        return response;
      case SIMPLE_SELECT_JOIN_TABLE:
        List<Lock> locks_simple_join = new ArrayList<>();
        try {
          SimpleJoinPlan simpleJoinPlan = (SimpleJoinPlan) plan;
          Table tableL = manager.getCurrentDatabase().findTableByName(simpleJoinPlan.getTableL());
          Table tableR = manager.getCurrentDatabase().findTableByName(simpleJoinPlan.getTableR());
          if (tableL == null || tableR == null) {
            throw new TableNotExistException(simpleJoinPlan.getTableL());
          }
          locks_simple_join.add(tableL.getReadLock());
          locks_simple_join.add(tableR.getReadLock());
          manager.transactionAddMultipleLocks(session_ID, locks_simple_join);
          QueryTable2 queryTable =
              manager
                  .getCurrentDatabase()
                  .selectSimpleJoin(
                      simpleJoinPlan.getTableL(),
                      simpleJoinPlan.getTableR(),
                      simpleJoinPlan.getJoinCondition(),
                      simpleJoinPlan.getWhereCondition());
          QueryResult2 queryResult =
              QueryResult2.makeResult(queryTable, simpleJoinPlan.getResultColumns());
          response = new ExecuteStatementResp(StatusUtil.success(), true);
          response.columnsList = queryResult.getResultColumnNames();
          response.rowList = queryResult.getRowsList();

        } catch (Exception e) {
          e.printStackTrace();
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          for (Lock t : locks_simple_join) {
            t.unlock();
          }
          execAutoCommit(session_ID);
        }
        return response;
      case SELECT_FROM_TABLE:
        List<Lock> locks = new ArrayList<>();
        try {
          // System.out.println("[DEBUG] " + plan);
          SelectPlan2 selectPlan = ((SelectPlan2) plan);
          for (SQLParser.TableQueryContext tableQuery : selectPlan.getTableQuerys()) {
            // 获取当前 TableQueryContext 对象中的所有 TableNameContext
            List<SQLParser.TableNameContext> queryTableNames = tableQuery.tableName();

            // 遍历 TableNameContext 列表
            for (SQLParser.TableNameContext tableNameContext : queryTableNames) {
              // 获取表名
              Table table =
                  manager.getCurrentDatabase().findTableByName(tableNameContext.getText());
              if (table == null) {
                throw new TableNotExistException(tableNameContext.getText());
              }
              ReentrantReadWriteLock.ReadLock lock = table.getReadLock();
              if (!locks.contains(lock)) {
                locks.add(lock);
              }
            }
          }
          manager.transactionAddMultipleLocks(session_ID, locks);

          QueryTable2 queryTable =
              manager
                  .getCurrentDatabase()
                  .select(selectPlan.getTableQuerys(), selectPlan.getMultipleCondition());
          QueryResult2 queryResult =
              QueryResult2.makeResult(queryTable, selectPlan.getResultColumns());
          response = new ExecuteStatementResp(StatusUtil.success(), true);
          response.columnsList = queryResult.getResultColumnNames();
          response.rowList = queryResult.getRowsList();

          //        System.out.println("[DEBUG] " + response.rowList);
        } catch (Exception e) {
          e.printStackTrace();
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        } finally {
          for (Lock t : locks) {
            t.unlock();
          }
          execAutoCommit(session_ID);
        }
        return response;

      case BEGIN_TRANSACTION:
        try {
          if (!manager.transactionSessions.contains(session_ID)) {
            manager.transactionSessions.add(session_ID);
          }
          manager.transactionBegin(session_ID);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(
            StatusUtil.success("Begin Transaction: " + String.valueOf(session_ID)), false);
      case COMMIT:
        try {
          manager.transactionCommit(session_ID);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(StatusUtil.success("Transaction committed"), false);
      case AUTO_BEGIN_TRANSACTION:
        try {
          manager.autoBegin(session_ID);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(StatusUtil.success("Transaction AutoBegined"), false);
      case AUTO_COMMIT:
        try {
          execAutoCommit(session_ID);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(StatusUtil.success("Transaction AutoCommitted"), false);
      default:
        break;
    }
    return null;
  }
}
