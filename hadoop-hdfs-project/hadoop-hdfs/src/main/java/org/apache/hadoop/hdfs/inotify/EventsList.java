/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.List;

/**
 * Contains a set of events and the transaction ID in the edit log up to which
 * we read to produce these events.
 */
@InterfaceAudience.Private
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
