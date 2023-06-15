package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Arrays;

public class PageRow extends Row {
  private Entry[] entries;

  public PageRow() {
    super();
  }

  public PageRow(Entry[] entries) {
    super();
    this.entries = entries;
  }

  @Override
  @Deprecated
  public void appendEntries(ArrayList<Entry> entries) throws RuntimeException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  @Deprecated
  public void addEntry(Entry entry) throws RuntimeException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  @Deprecated
  public void dropEntry(int index) throws RuntimeException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  @Deprecated
  public void alterEntryType(int index, Entry newEntry) throws RuntimeException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  @Deprecated
  public void alterEntryName(int index, Entry newEntry) throws RuntimeException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ArrayList<Entry> getEntries() {
    return new ArrayList<Entry>(Arrays.asList(entries));
  }
}
