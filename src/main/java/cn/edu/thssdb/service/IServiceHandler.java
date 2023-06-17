package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.LockQueueSleepException;
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
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  private Manager manager;
  private final String[] autoBeginStmt = {"insert", "delete", "update", "select", "alter"};
  private final String[] walStmt = {
    "insert", "delete", "update", "begin", "commit", "alter", "commit;"
  };

  public IServiceHandler() {
    super();
    manager = Manager.getInstance();
  }

  private void execAutoCommit(long session_ID) {
    // System.out.println("entering check auto commit status: " + manager.auto_dict.get(sessionID));
    if (manager.autoExecute.get(session_ID)) {
      autoCommit(session_ID);
    }
  }

  public ExecuteStatementResp autoBegin(long session_ID) {
    try {
      if (!manager.transactionSessions.contains(session_ID)) {
        manager.transactionSessions.add(session_ID);
        ArrayList<String> sLock = new ArrayList<>();
        ArrayList<String> xLock = new ArrayList<>();
        manager.xLockHash.put(session_ID, sLock);
        manager.sLockHash.put(session_ID, xLock);
      } else {
        return new ExecuteStatementResp(StatusUtil.fail("Session already in transaction"), false);
      }
    } catch (Exception e) {
      return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
    }
    return new ExecuteStatementResp(
        StatusUtil.success("Begin AutoTransaction: " + String.valueOf(session_ID)), false);
  }

  public ExecuteStatementResp autoCommit(long session_ID) {
    try {
      if (manager.transactionSessions.contains(session_ID)) {
        Database database = manager.getCurrentDatabase();
        manager.transactionSessions.remove(session_ID);

        ArrayList<String> xTableList = manager.xLockHash.get(session_ID);
        for (String tableName : xTableList) {
          Table table = database.findTableByName(tableName);
          table.freeXLock(session_ID);
        }
        xTableList.clear();
        manager.xLockHash.put(session_ID, xTableList);

        ArrayList<String> sTableList = manager.sLockHash.get(session_ID);
        for (String tableName : sTableList) {
          Table table = database.findTableByName(tableName);
          table.freeSLock(session_ID);
        }
        sTableList.clear();
        manager.sLockHash.put(session_ID, sTableList);
      } else {
        return new ExecuteStatementResp(
            StatusUtil.fail("Failed to auto commit, not transaction"), false);
      }
    } catch (Exception e) {
      return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
    }
    return new ExecuteStatementResp(StatusUtil.success("Transaction AutoCommitted"), false);
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
    manager.saveMetaDataToFile("metadata.meta");
    manager.saveTableDataToFile();

    return new DisconnectResp(StatusUtil.success());
  }

  //  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }

    System.out.println("execute" + req.getSessionId() + req.statement);
    long session_ID = req.getSessionId();
    String stmt = req.getStatement().split("\\s+")[0];
    System.out.println("stmt: " + stmt);

    if (!manager.transactionSessions.contains(session_ID)
        && Arrays.asList(autoBeginStmt).contains(stmt.toLowerCase())) {
      manager.autoExecute.put(session_ID, true);
      autoBegin(session_ID);
    } else {
      manager.autoExecute.put(session_ID, false);
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

          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          execAutoCommit(session_ID);
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
                              (column.getPrimary() != 0) ? "PRIMARY KEY" : "NOT PRIMARY",
                              (column.getNotNull()) ? "NOT NULL" : "CAN NULL",
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

          String tableName = alterTablePlan.getTableName();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          while (true) {
            if (table == null) {
              autoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new TableNotExistException(tableName).getMessage()), false);
            }
            if (!manager.lockQueue.contains(session_ID)) {
              int xLock = table.getXLock(session_ID);
              if (xLock != -1) {
                if (xLock == 1) {
                  ArrayList<String> temp = manager.xLockHash.get(session_ID);
                  temp.add(tableName);
                  manager.xLockHash.put(session_ID, temp);
                }
                break;
              } else {
                manager.lockQueue.add(session_ID);
              }
            } else {
              if (manager.lockQueue.get(0) == session_ID) {
                int xLock_queue = table.getXLock(session_ID);
                if (xLock_queue != -1) {
                  if (xLock_queue == 1) {
                    ArrayList<String> tmp = manager.xLockHash.get(session_ID);
                    tmp.add(tableName);
                    manager.xLockHash.put(session_ID, tmp);
                  }
                  manager.lockQueue.remove(0);
                  break;
                }
              }
            }

            try {
              Thread.sleep(200);
            } catch (Exception e) {
              execAutoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new LockQueueSleepException().getMessage()), false);
            }
          }

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
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }

      case INSERT_INTO_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          InsertPlan insertPlan = (InsertPlan) plan;

          String tableName = insertPlan.getTableName();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          while (true) {
            if (table == null) {
              autoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new TableNotExistException(tableName).getMessage()), false);
            }
            if (!manager.lockQueue.contains(session_ID)) {
              int xLock = table.getXLock(session_ID);
              if (xLock != -1) {
                if (xLock == 1) {
                  ArrayList<String> temp = manager.xLockHash.get(session_ID);
                  temp.add(tableName);
                  manager.xLockHash.put(session_ID, temp);
                }
                break;
              } else {
                manager.lockQueue.add(session_ID);
              }
            } else {
              if (manager.lockQueue.get(0) == session_ID) {
                int xLock_queue = table.getXLock(session_ID);
                if (xLock_queue != -1) {
                  if (xLock_queue == 1) {
                    ArrayList<String> tmp = manager.xLockHash.get(session_ID);
                    tmp.add(tableName);
                    manager.xLockHash.put(session_ID, tmp);
                  }
                  manager.lockQueue.remove(0);
                  break;
                }
              }
            }

            try {
              Thread.sleep(200);
            } catch (Exception e) {
              execAutoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new LockQueueSleepException().getMessage()), false);
            }
          }

          manager.insertIntoTable(
              insertPlan.getTableName(), insertPlan.getColumnNames(), insertPlan.getValues());
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DELETE_FROM_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          DeletePlan deletePlan = (DeletePlan) plan;
          String tableName = deletePlan.getTableName();
          List<String> conditions = deletePlan.getConditions();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          while (true) {
            if (table == null) {
              autoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new TableNotExistException(tableName).getMessage()), false);
            }
            if (!manager.lockQueue.contains(session_ID)) {
              int xLock = table.getXLock(session_ID);
              if (xLock != -1) {
                if (xLock == 1) {
                  ArrayList<String> temp = manager.xLockHash.get(session_ID);
                  temp.add(tableName);
                  manager.xLockHash.put(session_ID, temp);
                }
                break;
              } else {
                manager.lockQueue.add(session_ID);
              }
            } else {
              if (manager.lockQueue.get(0) == session_ID) {
                int xLock_queue = table.getXLock(session_ID);
                if (xLock_queue != -1) {
                  if (xLock_queue == 1) {
                    ArrayList<String> tmp = manager.xLockHash.get(session_ID);
                    tmp.add(tableName);
                    manager.xLockHash.put(session_ID, tmp);
                  }
                  manager.lockQueue.remove(0);
                  break;
                }
              }
            }

            try {
              Thread.sleep(200);
            } catch (Exception e) {
              execAutoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new LockQueueSleepException().getMessage()), false);
            }
          }

          manager.deleteFromTable(tableName, conditions);

          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case UPDATE_TABLE:
        try {
          System.out.println("[DEBUG] " + plan);
          UpdatePlan updatePlan = (UpdatePlan) plan;
          String tableName = updatePlan.getTableName();
          String columnName = updatePlan.getColumnName();
          String newValue = updatePlan.getNewValue();
          List<String> conditions = updatePlan.getConditions();
          Table table = manager.getCurrentDatabase().findTableByName(tableName);

          while (true) {
            if (table == null) {
              autoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new TableNotExistException(tableName).getMessage()), false);
            }
            if (!manager.lockQueue.contains(session_ID)) {
              int xLock = table.getXLock(session_ID);
              if (xLock != -1) {
                if (xLock == 1) {
                  ArrayList<String> temp = manager.xLockHash.get(session_ID);
                  temp.add(tableName);
                  manager.xLockHash.put(session_ID, temp);
                }
                break;
              } else {
                manager.lockQueue.add(session_ID);
              }
            } else {
              if (manager.lockQueue.get(0) == session_ID) {
                int xLock_queue = table.getXLock(session_ID);
                if (xLock_queue != -1) {
                  if (xLock_queue == 1) {
                    ArrayList<String> tmp = manager.xLockHash.get(session_ID);
                    tmp.add(tableName);
                    manager.xLockHash.put(session_ID, tmp);
                  }
                  manager.lockQueue.remove(0);
                  break;
                }
              }
            }

            try {
              Thread.sleep(200);
            } catch (Exception e) {
              execAutoCommit(session_ID);
              return new ExecuteStatementResp(
                  StatusUtil.fail(new LockQueueSleepException().getMessage()), false);
            }
          }

          manager.updateTable(tableName, columnName, newValue, conditions);
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          execAutoCommit(session_ID);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case SELECT_FROM_TABLE:
        System.out.println("[DEBUG] " + plan);
        SelectPlan2 selectPlan = ((SelectPlan2) plan);

        List<String> tableNames = new ArrayList<>();
        for (SQLParser.TableQueryContext tableQuery : selectPlan.getTableQuerys()) {
          // 获取当前 TableQueryContext 对象中的所有 TableNameContext
          List<SQLParser.TableNameContext> queryTableNames = tableQuery.tableName();

          // 遍历 TableNameContext 列表
          for (SQLParser.TableNameContext tableName : queryTableNames) {
            // 获取表名
            tableNames.add(tableName.getText());
          }
        }

        while (true) {
          if (!manager.lockQueue.contains(session_ID)) {
            ArrayList<Integer> lockList = new ArrayList<>();
            for (String tableName : tableNames) {
              Table table = manager.getCurrentDatabase().findTableByName(tableName);
              if (table == null) {
                autoCommit(session_ID);
                return new ExecuteStatementResp(
                    StatusUtil.fail(new TableNotExistException(tableName).getMessage()), false);
              }

              int sLock = table.getSLock(session_ID);
              lockList.add(sLock);
            }

            if (lockList.contains(-1)) {
              for (String tableName : tableNames) {
                Table table = manager.getCurrentDatabase().findTableByName(tableName);
                table.freeSLock(session_ID);
              }
              manager.lockQueue.add(session_ID);
            } else {
              break;
            }
          } else {
            if (manager.lockQueue.get(0) == session_ID) {
              ArrayList<Integer> lockList = new ArrayList<>();
              for (String tableName : tableNames) {
                Table table = manager.getCurrentDatabase().findTableByName(tableName);
                if (table == null) {
                  autoCommit(session_ID);
                  return new ExecuteStatementResp(
                      StatusUtil.fail((new TableNotExistException(tableName).getMessage())), false);
                }

                int sLock = table.getSLock(session_ID);
                lockList.add(sLock);
              }

              if (!lockList.contains(-1)) {
                manager.lockQueue.remove(0);
                break;
              } else {
                for (String tableName : tableNames) {
                  Table table = manager.getCurrentDatabase().findTableByName(tableName);
                  table.freeSLock(session_ID);
                }
              }
            }
          }
          try {
            Thread.sleep(200);
          } catch (Exception e) {
            autoCommit(session_ID);
            return new ExecuteStatementResp(
                StatusUtil.fail(new LockQueueSleepException().getMessage()), false);
          }
        }
        for (String tableName : tableNames) {
          Table table = manager.getCurrentDatabase().findTableByName(tableName);
          table.freeSLock(session_ID);
        }

        QueryTable2 queryTable =
            manager
                .getCurrentDatabase()
                .select(selectPlan.getTableQuerys(), selectPlan.getMultipleCondition());
        //        QueryResult queryResult =
        //            manager
        //                .getCurrentDatabase()
        //                .select(
        //                    selectPlan.getQueryTables(),
        //                    selectPlan.getMetaInfos(),
        //                    selectPlan.getJoinCondition(),
        //                    selectPlan.getWhereCondition());
        QueryResult2 queryResult =
            QueryResult2.makeResult(queryTable, selectPlan.getResultColumns());
        ExecuteStatementResp res = new ExecuteStatementResp(StatusUtil.success(), true);
        res.columnsList = queryResult.getResultColumnNames();
        res.rowList = queryResult.getRowsList();

        execAutoCommit(session_ID);
        return res;

      case BEGIN_TRANSACTION:
        try {
          if (!manager.transactionSessions.contains(session_ID)) {
            manager.transactionSessions.add(session_ID);
            ArrayList<String> sLock = new ArrayList<>();
            ArrayList<String> xLock = new ArrayList<>();
            manager.xLockHash.put(session_ID, sLock);
            manager.sLockHash.put(session_ID, xLock);
          } else {
            return new ExecuteStatementResp(
                StatusUtil.fail("Session already in transaction!"), false);
          }
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(
            StatusUtil.success("Begin Transaction: " + String.valueOf(session_ID)), false);
      case COMMIT:
        try {
          if (manager.transactionSessions.contains(session_ID)) {
            Database database = manager.getCurrentDatabase();
            String dbName = database.getName();
            manager.transactionSessions.remove(session_ID);

            ArrayList<String> xTableList = manager.xLockHash.get(session_ID);
            for (String tableName : xTableList) {
              Table table = database.findTableByName(tableName);
              table.freeXLock(session_ID);
            }
            xTableList.clear();
            manager.xLockHash.put(session_ID, xTableList);

            ArrayList<String> sTableList = manager.sLockHash.get(session_ID);
            if (sTableList.size() != 0) {
              for (String tableName : sTableList) {
                Table table = database.findTableByName(tableName);
                table.freeSLock(session_ID);
              }
              sTableList.clear();
              manager.sLockHash.put(session_ID, sTableList);
            }
          } else {
            return new ExecuteStatementResp(
                StatusUtil.fail("Failed to commit, not transaction"), false);
          }
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(StatusUtil.success("Transaction committed"), false);
      case AUTO_BEGIN_TRANSACTION:
        return autoBegin(session_ID);
      case AUTO_COMMIT:
        return autoCommit(session_ID);
      default:
        break;
    }
    return null;
  }
}
