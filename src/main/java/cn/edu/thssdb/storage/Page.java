package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Manager;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Page<V extends Cloneable<V>> implements Serializable, PageInterface {
  private static final long serialVersionUID = -5809782578272943999L;

  transient ArrayList<V> rows;
  transient boolean load;
  transient boolean modified;
  transient Storage storage;
  transient long lastVisit;
  transient ReentrantReadWriteLock lock;
  transient ReentrantLock loadLock;

  private final String identifier;

  private final UUID uuid;

  public UUID getUuid() {
    return uuid;
  }

  public Page(List<V> list, String identifier, UUID uuid) {
    //    System.out.println("Create Page "+uuid);
    this.rows = new ArrayList<>(list);
    this.identifier = identifier;
    this.uuid = uuid;
    this.load = true;
    this.modified = false;
    this.storage = Manager.getInstance().getStorage();
    this.lock = new ReentrantReadWriteLock();
    this.loadLock = new ReentrantLock();
    updateLastVisit();
    this.storage.addPage(this);
  }

  @Override
  public long getLastVisit() {
    return lastVisit;
  }

  private void updateLastVisit() {
    this.lastVisit = System.currentTimeMillis();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.load = true;
    this.modified = false;
    this.storage = Manager.getInstance().getStorage();
    this.lock = new ReentrantReadWriteLock();
    this.loadLock = new ReentrantLock();
    updateLastVisit();
  }

  public void set(int index, V value) {
    try {
      this.loadLock.lock();
      if (!load) {
        loadPage();
        load = true;
      }
      updateLastVisit();
      this.lock.writeLock().lock();
      this.loadLock.unlock();
      rows.set(index, value);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public V get(int index) {
    V result;
    try {
      this.loadLock.lock();
      if (!load) {
        loadPage();
        load = true;
      }
      updateLastVisit();
      this.lock.readLock().lock();
      this.loadLock.unlock();
      result = rows.get(index);
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public List<V> getallClone(int length) {
    List<V> result = new ArrayList<>();
    try {
      this.loadLock.lock();
      if (!load) {
        loadPage();
        load = true;
      }
      updateLastVisit();
      this.lock.readLock().lock();
      this.loadLock.unlock();
      for (int i = 0; i < length; i++) {
        result.add(rows.get(i).clone());
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void loadPage() {
    //    System.out.println("Try write loadpage lock " + getPageFileName());
    lock.writeLock().lock();
    //    System.out.println("Get write loadpage lock " + getPageFileName());
    Path filePath = Paths.get("./data", getPageFileName());
    try (FileInputStream fis = new FileInputStream(filePath.toFile());
        BufferedInputStream bis = new BufferedInputStream(fis);
        //         FileChannel inChannel = fis.getChannel()
        ) {

      //      ByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());

      //      ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array());
      ObjectInputStream ois = new ObjectInputStream(bis);

      this.rows = (ArrayList<V>) ois.readObject();
      this.load = true;
      storage.addPage(this);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getIdentifier() {
    return getPageFileName();
  }

  public UUID getUUID() {
    return uuid;
  }

  @Override
  public void release() {
    loadLock.lock();
    //    System.out.println("Try write lock " + getPageFileName());
    lock.writeLock().lock();
    //    System.out.println("Get write lock " + getPageFileName());
    try {
      persist();
      rows = null;
      this.load = false;
    } finally {
      lock.writeLock().unlock();
      loadLock.unlock();
    }
  }

  @Override
  public void persist() {
    Path filePath = Paths.get("./data", getPageFileName());
    try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(rows);
      oos.flush();
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getPageFileName() {
    return getPageFileName(identifier, uuid);
  }

  public static String getPageFileName(String identifier, UUID uuid) {
    return identifier + "_" + uuid.toString() + ".data";
  }

  public ReentrantReadWriteLock.WriteLock getReadLock() {
    //    System.out.println("Get read lock "+getPageFileName());
    return lock.writeLock();
  }
}
