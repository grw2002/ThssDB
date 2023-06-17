package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PageRow extends Row implements Cloneable<PageRow> {

  public PageRow() {
    super();
  }

  public PageRow(Entry[] entries) {
    super(entries);
  }

  public PageRow(Row row) {
    super(row.getEntries().toArray(new Entry[0]));
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
  public final List<Entry> getEntries() {
    return Collections.unmodifiableList(entries);
  }

  public void setEntries(List<Entry> entries) {
    this.entries = (ArrayList<Entry>) entries;
  }

  public void updateEntry(int index, Entry newValue) {
    this.entries.set(index, newValue);
  }

  @Override
  public PageRow clone() {
    PageRow row = new PageRow();
    for (Entry entry : entries) {
      row.entries.add(new Entry(entry.value));
    }
    return row;
  }
}
