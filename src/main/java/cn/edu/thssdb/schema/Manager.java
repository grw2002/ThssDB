package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistsException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.Storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private final Storage storage;

  public Storage getStorage() {
    return storage;
  }

  public void beginTransaction() {
    lock.writeLock().lock();
    inTransaction = true;
  }

  public void commit() {
    lock.writeLock().unlock();
    inTransaction = false;
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
          }
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

  public static Manager getInstance() {
    Manager instance = Manager.ManagerHolder.INSTANCE;
    return instance;
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

  public List<String> getTablesName() {
    return currentDatabase.getTablesName();
  }

  public void createTable(String tableName, List<Column> columns) throws RuntimeException {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    currentDatabase.create(tableName, columns);
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
