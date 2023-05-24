package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistsException;
import cn.edu.thssdb.exception.DatabaseNotExistException;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private Database currentDatabase;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
    currentDatabase = null;
  }

  public Database getCurrentDatabase() {
    return currentDatabase;
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

  private void deleteDatabase(String databaseName) throws RuntimeException {
    // TODO
    if (databases.containsKey(databaseName)) {
      if (currentDatabase.getName() == databaseName) {
        currentDatabase = null;
      }
      databases.remove(databaseName);
    } else {
      throw new DatabaseNotExistException();
    }
  }

  public void switchDatabase(String databaseName) throws RuntimeException {
    // TODO
    if (databases.containsKey(databaseName)) {
      currentDatabase = databases.get(databaseName);
    } else {
      throw new DatabaseNotExistException();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
