package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistsException;
import cn.edu.thssdb.exception.DatabaseNotExistException;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Database currentDatabase;

  /* Persistence
   * saveMetaDataToFile & loadMetaDataFromFile
   */
  public void saveMetaDataToFile(String filePath) {
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
      oos.writeObject(this.databases);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void loadMetaDataFromFile(String filePath) {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
      this.databases = (HashMap<String, Database>) ois.readObject();
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
    }

    instance.loadMetaDataFromFile(filePath.toString());
    return instance;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
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

    saveMetaDataToFile("metadata.meta");
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
      if (currentDatabase.getName() == databaseName) {
        currentDatabase = null;
      }
      databases.remove(databaseName);
    } else {
      throw new DatabaseNotExistException();
    }

    saveMetaDataToFile("metadata.meta");
  }

  public void switchDatabase(String databaseName) throws RuntimeException {
    // TODO
    if (databases.containsKey(databaseName)) {
      currentDatabase = databases.get(databaseName);
    } else {
      throw new DatabaseNotExistException();
    }
  }

  public List<String> showDatabases() throws RuntimeException {
    return new ArrayList<>(databases.keySet());
  }

  public void createTable(String tableName, List<Column> columns) throws RuntimeException {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    Column[] columnArray = columns.toArray(new Column[columns.size()]);
    currentDatabase.create(tableName, columnArray);

    saveMetaDataToFile("metadata.meta");
  }

  public void dropTable(String tableName, boolean ifExists) {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    if (!ifExists) {
      currentDatabase.drop(tableName);
    }

    saveMetaDataToFile("metadata.meta");
  }

  public List<Column> showTable(String tableName) {
    if (currentDatabase == null) {
      throw new RuntimeException("No database selected");
    }

    List<Column> columns = currentDatabase.getTableColumns(tableName);
    return columns;
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
