package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Manager;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Page<V> implements Serializable, PageInterface {
  private static final long serialVersionUID = -5809782578272943999L;

  transient ArrayList<V> rows;
  transient boolean load;
  transient boolean modified;
  transient Storage storage;
  transient long lastVisit;

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
    updateLastVisit();
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
    this.storage = Manager.getInstance().getStorage();
  }

  public void set(int index, V value) {
    if (!load) {
      loadPage();
    }
    this.modified = true;
    rows.set(index, value);
    updateLastVisit();
  }

  public V get(int index) {
    //    System.out.println("Page "+uuid+" "+load);
    if (!load) {
      loadPage();
    }
    updateLastVisit();
    return rows.get(index);
  }

  public void loadPage() {
    try (FileInputStream fis = new FileInputStream(getPageFileName(identifier, uuid));
        FileChannel inChannel = fis.getChannel()) {

      ByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());

      ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array());
      ObjectInputStream ois = new ObjectInputStream(bis);

      this.rows = (ArrayList<V>) ois.readObject();
      this.load = true;

    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
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
    //    if (modified) {
    persist();
    //    }
    rows = null;
    this.load = false;
  }

  @Override
  public void persist() {
    try (FileOutputStream fos = new FileOutputStream(getPageFileName())) {
      //      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(rows);
      oos.close();
      fos.close();
      //      byte[] data = bos.toByteArray();
      //      fos.write(data);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getPageFileName() {
    return getPageFileName(identifier, uuid);
  }

  public static String getPageFileName(String DatabaseName, UUID uuid) {
    return DatabaseName + "_" + uuid.toString() + ".data";
  }
}
