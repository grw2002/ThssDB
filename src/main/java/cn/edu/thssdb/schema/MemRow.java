package cn.edu.thssdb.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MemRow extends Row {
  //  protected static final long serialVersionUID = -5709782578272943998L;
  //  private int pageId;
  //  private int offset;
  private transient List<Entry> entries;

  //  public void setPage(int pageId, int offset) {
  //    this.pageId = pageId;
  //    this.offset = offset;
  //  }
  //
  //  public void setPage(Page page, int position) {
  //    setPage(page.getID(), position);
  //  }

  public MemRow() {
    super();
    this.entries = new ArrayList<Entry>();
  }

  public MemRow(Entry[] entries) {
    super();
    this.entries = new ArrayList<Entry>(Arrays.asList(entries));
    // TODO
  }

  @Override
  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  @Override
  public void addEntry(Entry entry) {
    this.entries.add(entry);
  }

  @Override
  public void dropEntry(int index) {
    this.entries.remove(index);
  }

  @Override
  public void alterEntryType(int index, Entry newEntry) {
    this.entries.set(index, newEntry);
  }

  @Override
  public void alterEntryName(int index, Entry newEntry) {
    this.entries.set(index, newEntry);
  }

  @Override
  public List<Entry> getEntries() {
    return entries;
  }
}
