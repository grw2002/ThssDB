package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Storage {
  static final int MAX_PAGES = 1024;
  final HashMap<UUID, PageInterface> pages;

  public Storage() {
    this.pages = new HashMap<>();
  }

  public void addPage(PageInterface page) {
    if (pages.size() >= MAX_PAGES) {
      long minx = Long.MAX_VALUE;
      UUID target = null;
      for (Map.Entry<UUID, PageInterface> uuidPageInterfaceEntry : pages.entrySet()) {
        long x = uuidPageInterfaceEntry.getValue().getLastVisit();
        if (x < minx) {
          target = uuidPageInterfaceEntry.getKey();
          minx = x;
        }
      }
      PageInterface oldPage = pages.get(target);
      oldPage.release();
      pages.remove(target);
    }
    pages.put(page.getUUID(), page);
  }

  public Row getRow(Pair<Integer, Integer> pos) {
    // TODO
    return null;
  }
}
