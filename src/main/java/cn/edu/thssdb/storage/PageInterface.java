package cn.edu.thssdb.storage;

import java.util.UUID;

public interface PageInterface {
  public void release();

  public void persist();

  public String getIdentifier();

  public UUID getUUID();

  public long getLastVisit();
}
