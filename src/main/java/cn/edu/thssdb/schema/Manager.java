package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistsException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;

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

  /* Lock and unlock
   */

  public void beginTransaction() {
    lock.writeLock().lock();
    inTransaction = true;
  }

  public void commit() {
    lock.writeLock().unlock();
    inTransaction = false;
  }

  /* Persistence
   * saveMetaDataToFile & loadMetaDataFromFile
   */
  public void saveTableDataToFile() {
    if (currentDatabase == null) return;
    for (Table table : currentDatabase.getTables()) {
      // 保存每个表的数据
      table.saveTableDataToFile();
    }
  }

  public void saveMetaDataToFile(String filePath) {
    try (FileOutputStream fos = new FileOutputStream(filePath);
        GZIPOutputStream gos = new GZIPOutputStream(fos);
        ObjectOutputStream oos = new ObjectOutputStream(gos)) {

      oos.writeObject(this.databases);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void loadMetaDataFromFile(String filePath) {
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
            for (Table table : database.getTables()) {
              table.initTransientFields();
            }
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
    String fileName = "metadata.meta";
    Path filePath = Paths.get(fileName);

    if (!Files.exists(filePath)) {
      // File not Exist -> Create File
      try {
        Files.createFile(filePath);
      } catch (IOException e) {
        // Handle Exception
        e.printStackTrace();
      }
      return instance;
    }

    instance.loadMetaDataFromFile(filePath.toString());
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
      saveTableDataToFile();
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

    Column[] columnArray = columns.toArray(new Column[columns.size()]);
    currentDatabase.create(tableName, columnArray);
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

    table.loadTableDataFromFile();
    return table.getAllRowsInfo();
  }

  public void insertIntoTable(
      String tableName, List<String> columnNames, List<List<String>> values) {
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.loadTableDataFromFile();
    table.insertNameValue(columnNames, values);
  }

  public void deleteFromTable(String tableName, List<String> conditions) {
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.loadTableDataFromFile();
    table.deleteWithConditions(conditions);
  }

  public void updateTable(
      String tableName, String columnName, String newValue, List<String> conditions) {
    // Find the table
    Table table = this.currentDatabase.findTableByName(tableName);

    if (table == null) {
      throw new TableNotExistException(tableName);
    }

    table.loadTableDataFromFile();
    // Call the new update method in Table class
    table.updateWithConditions(columnName, newValue, conditions);
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
