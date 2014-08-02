package org.apache.hadoop.hdfs.inotify;

import java.util.List;

public class EventsList {
  private List<Event> events;
  private long lastTxid;

  public EventsList(List<Event> events, long lastTxid) {
    this.events = events;
    this.lastTxid = lastTxid;
  }

  public List<Event> getEvents() {
    return events;
  }

  public long getLastTxid() {
    return lastTxid;
  }
}
