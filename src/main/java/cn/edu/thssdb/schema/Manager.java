package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistsException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.LogFileException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.Storage;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private boolean inTransaction = false;
  private Database currentDatabase;
  public ArrayList<Long> transactionSessions;
  public ArrayList<Long> lockQueue;
  public HashMap<Long, Boolean> autoExecute;
  public HashMap<Long, ArrayList<String>> xLockHash;
  public HashMap<Long, ArrayList<String>> sLockHash;
  public HashMap<Long, List<Lock>> tranLocks;
  private final Storage storage;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
    autoExecute = new HashMap<>();
    xLockHash = new HashMap<>();
    sLockHash = new HashMap<>();
    transactionSessions = new ArrayList<>();
    lockQueue = new ArrayList<>();
    currentDatabase = null;
    this.storage = new Storage();
    this.tranLocks = new HashMap<>();
  }

  public Storage getStorage() {
    return storage;
  }

  public List<Lock> getTranLocks(Long sessionId) {
    if (tranLocks.containsKey(sessionId)) {
      return tranLocks.get(sessionId);
    }
    return null;
  }

  public void transactionBegin(long sessionId) {
    if (!autoExecute.containsKey(sessionId)) {
      autoExecute.put(sessionId, false);
    }
    if (!tranLocks.containsKey(sessionId) || tranLocks.get(sessionId) == null) {
      tranLocks.put(sessionId, new ArrayList<>());
    } else if (tranLocks.get(sessionId).size() > 0) {
      throw new RuntimeException("transaction already begun");
    }
  }

  public void transactionCommit(long sessionId) {
    if (autoExecute.containsKey(sessionId)) {
      autoExecute.remove(sessionId);
      for (Lock lock1 : tranLocks.get(sessionId)) {
        lock1.unlock();
      }
      tranLocks.remove(sessionId);
    } else {
      throw new RuntimeException("transaction not begun");
    }
  }

  public void transactionAddLock(long sessionId, Lock lock) {
    List<Lock> locks = tranLocks.get(sessionId);
    if (locks.contains(lock)) {
      return;
    }
    lock.lock();
    locks.add(lock);
  }

  public void autoCommit(long sessionId) {
    if (autoExecute.containsKey(sessionId)) {
      if (autoExecute.get(sessionId)) {
        transactionCommit(sessionId);
      }
    }
  }

  public void autoBegin(long sessionId) {
    if (!autoExecute.containsKey(sessionId)) {
      autoExecute.put(sessionId, true);
      transactionBegin(sessionId);
    }
  }

  public void persist(String filePath) {
    try (FileOutputStream fos = new FileOutputStream(filePath);
        GZIPOutputStream gos = new GZIPOutputStream(fos);
        ObjectOutputStream oos = new ObjectOutputStream(gos)) {

      oos.writeObject(this.databases);
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (Database database : databases.values()) {
      database.persist();
    }
  }

  public void recover(String filePath) {
    Path loadPath = Paths.get(filePath);
    try {
      if (Files.size(loadPath) == 0) {
        System.out.println("Empty metadata file. No existing databases.");
        return; // 如果文件为空，直接返回，不创建 ObjectInputStream
      }
    } catch (IOException e) {
      e.printStackTrace();
      return; // 如果发生异常，直接返回
    }
    try (FileInputStream fis = new FileInputStream(filePath);
        GZIPInputStream gis = new GZIPInputStream(fis);
        ObjectInputStream ois = new ObjectInputStream(gis)) {

      Object fileContent = ois.readObject();
      if (fileContent != null) {
        if (fileContent instanceof HashMap) {
          this.databases = (HashMap<String, Database>) fileContent;
          for (Database database : this.databases.values()) {
            database.recover();

            // recover from log
            if(Global.WAL_SWITCH) readLog(database.getName());
          }
          this.currentDatabase = null;
        } else {
          System.out.println("Invalid metadata file content. Expected HashMap<String, Database>.");
        }
      } else {
        System.out.println("Empty metadata file. Keeping existing databases.");
      }
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void writeLog(String statement, long session_ID) {
    if (this.currentDatabase == null) {
      return;
    }
    String logFile = this.getCurrentDatabaseName() + ".log";

    try {
      // session_ID: statement
      FileWriter writer = new FileWriter(logFile, true);
      writer.write(session_ID + ":" + statement + "\n");
      writer.close();
    } catch (Exception e) {
      throw new LogFileException(this.getCurrentDatabaseName());
    }
  }

  public void deleteLog(String databaseName) {
    System.out.println("deleting log: " + databaseName);

    String logFileName = databaseName + ".log";
    File logFile = new File(logFileName);
    try {
      logFile.delete();
    } catch (Exception e) {
      throw new LogFileException(databaseName);
    }
  }

  public void readLog(String databaseName) {
    String logFileName = databaseName + ".log";
    File logFile = new File(logFileName);

    if (!logFile.isFile() || !logFile.exists()) {
      throw new LogFileException(databaseName);
    }
    try {
      currentDatabase = databases.get(databaseName);

      InputStreamReader reader = new InputStreamReader(new FileInputStream(logFile));
      BufferedReader bufReader = new BufferedReader(reader);

      ArrayList<String> logRecords = new ArrayList<>();
      String record;

      // begin without commit
      HashMap<String, Integer> firstBeginWithoutCommit = new HashMap<>();
      int index = 0;

      try {
        while ((record = bufReader.readLine()) != null) {
          String session_ID = record.split(":")[0].trim();
          String stmt = record.split(":")[1].trim();

          if (stmt.equals("commit") || (stmt.equals("commit;"))) {
            firstBeginWithoutCommit.remove(session_ID);
          } else if (stmt.equals("begin transaction") || (stmt.equals("begin transaction;"))) {
            if (!firstBeginWithoutCommit.containsKey(session_ID)) {
              firstBeginWithoutCommit.put(session_ID, index);
            }
          }
          logRecords.add(record);
          index++;
        }

        // redo
        ArrayList<String> executedLines = new ArrayList<>();

        ExecuteStatementReq useDataBaseReq = new ExecuteStatementReq();
        useDataBaseReq.setStatement("use " + databaseName + ";");
        useDataBaseReq.setSessionId(Global.ADMINISTER_SESSION);
        IServiceHandler handler = new IServiceHandler(this);
        handler.executeStatement(useDataBaseReq);

        for (int i = 0; i < logRecords.size(); i++) {
          String session_ID = logRecords.get(i).split(":")[0].trim();
          String stmt = logRecords.get(i).split(":")[1].trim();

          boolean shouldExecute = false;
          System.out.println(stmt);
          if (firstBeginWithoutCommit.containsKey(session_ID)
              && firstBeginWithoutCommit.get(session_ID) > i) {
            shouldExecute = true;
          } else if (!firstBeginWithoutCommit.containsKey(session_ID)) {
            shouldExecute = true;
          }

          if (shouldExecute) {
            ExecuteStatementReq req = new ExecuteStatementReq();
            req.setStatement(stmt);
            req.setSessionId(Global.ADMINISTER_SESSION);
            handler.executeStatement(req);
            String executeLine = session_ID + ":" + stmt + "\n";
            executedLines.add(executeLine);
          }
        }
        bufReader.close();
        reader.close();

        // alter log
        if (executedLines.size() != logRecords.size()) {
          FileWriter writer = new FileWriter(logFile);
          writer.write("");
          writer.close();
          FileWriter newWriter = new FileWriter(logFile, true);

          for (String executedLine : executedLines) {
            newWriter.write(executedLine);
          }
          newWriter.close();
        }
      } catch (Exception e) {
        throw new LogFileException(e.getMessage());
      }
    } catch (Exception e) {
      throw new LogFileException(e.getMessage());
    }
  }

  public Database getCurrentDatabase() {
    return currentDatabase;
  }

  public String getCurrentDatabaseName() {
    if (currentDatabase == null) {
      return "null";
    }

    return currentDatabase.getName();
  }

  public void createDatabase(String databaseName) throws RuntimeException {
    if (databases.containsKey(databaseName)) {
      throw new DatabaseExistsException();
    }
    createDatabaseIfNotExists(databaseName);
  }

  private void createDatabaseIfNotExists(String databaseName) {
    // TODO
    Database newDB = new Database(databaseName);
    databases.put(databaseName, newDB);
    currentDatabase = newDB;
  }

  public void deleteDatabase(String databaseName) throws RuntimeException {
    // TODO
    if (databases.containsKey(databaseName)) {
      if (currentDatabase.getName().equals(databaseName)) {
        currentDatabase = null;
      }
      if(Global.WAL_SWITCH) deleteLog(databaseName);
      databases.remove(databaseName);
    } else {
      throw new DatabaseNotExistException();
    }
  }

  public void switchDatabase(String databaseName) throws RuntimeException {
    // 如果当前数据库不为空
    if (currentDatabase != null) {
      currentDatabase.persist();
    }

    // 切换数据库
    if (databases.containsKey(databaseName)) {
      currentDatabase = databases.get(databaseName);
    } else {
      throw new DatabaseNotExistException();
    }
  }

  public List<String> showDatabases() throws RuntimeException {
    return new ArrayList<>(databases.keySet());
  }

  public List<String> getTablesName(String databaseName) {
    return findDatabaseByName(databaseName).getTablesName();
  }

  public void createTable(String tableName, List<Column> columns) throws RuntimeException {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    currentDatabase.writeLock();
    try {
      currentDatabase.create(tableName, columns);
    } finally {
      currentDatabase.writeUnlock();
    }
  }

  public void dropTable(String tableName, boolean ifExists) {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    if (!ifExists) {
      currentDatabase.drop(tableName);
    }
  }

  public List<Column> showTable(String tableName) {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    return currentDatabase.getTableColumns(tableName);
  }

  public void addColumn(String tableName, Column column) {
    Table table = currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new RuntimeException("Table " + tableName + " not found");
    }

    table.addColumn(column);
  }

  public void dropColumn(String tableName, String columnName) {
    Table table = currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new RuntimeException("Table " + tableName + " not found");
    }

    table.dropColumn(columnName);
  }

  public void alterColumnType(String tableName, String columnName, String newColumnType) {
    Table table = currentDatabase.findTableByName(tableName);
    if (table == null) {
      throw new RuntimeException("Table " + tableName + " not found");
    }

    table.alterType(columnName, newColumnType);
  }

  public void renameColumn(String tableName, String columnName, String newColumnName) {
    Table table = currentDatabase.findTableByName(tableName);
    if (table == null) {
      throw new RuntimeException("Table " + tableName + " not found");
    }

    table.alterName(columnName, newColumnName);
  }

  public List<String> showRowsInTable(String tableName) {
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.recover();
    return table.getAllRowsInfo();
  }

  public void insertIntoTable(
      String tableName, List<String> columnNames, List<List<String>> values) {
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.recover();
    table.insertNameValue(columnNames, values);
  }

  public void deleteFromTable(String tableName, List<String> conditions) {
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.recover();
    table.deleteWithConditions(conditions);
  }

  public void updateTable(
      String tableName,
      String columnName,
      SQLParser.LiteralValueContext newValue,
      SQLParser.ConditionContext condition) {
    // Find the table
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.recover();
    // Call the new update method in Table class
    table.updateWithConditions(columnName, newValue, condition);
  }

  public Database findDatabaseByName(String databaseName) throws RuntimeException {
    if (databases.containsKey(databaseName)) {
      return databases.get(databaseName);
    }
    throw new DatabaseNotExistException();
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
