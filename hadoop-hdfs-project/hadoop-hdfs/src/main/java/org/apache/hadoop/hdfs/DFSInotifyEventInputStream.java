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

package org.apache.hadoop.hdfs;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventsList;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Stream for reading inotify events. DFSInotifyEventInputStreams should not
 * be shared among multiple threads.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DFSInotifyEventInputStream {
  private final ClientProtocol namenode;
  private Iterator<Event> it;
  private boolean notifyMissingEvents;
  private long lastReadTxid;
  // the most recent txid the NameNode told us it has sync'ed -- helps us
  // determine how far behind we are in the edit stream
  private long syncTxid;
  // we want a cached thread pool rather than a single thread pool because
  // when a call to the timed poll returns, its Callable may not actually have
  // finished executing, so the Callable of a subsequent call to the timed poll
  // would need a fresh thread to begin executing immediately
  private ExecutorService ex = Executors.newCachedThreadPool();

  private static final long INITIAL_WAIT_MS = 10;

  DFSInotifyEventInputStream(ClientProtocol namenode) throws IOException {
    this(namenode, namenode.getCurrentEditLogTxid()); // only consider new txn's
  }

  DFSInotifyEventInputStream(ClientProtocol namenode, long lastReadTxid)
      throws IOException {
    this.namenode = namenode;
    this.it = Iterators.emptyIterator();
    this.lastReadTxid = lastReadTxid;
  }

  /**
   * Returns the next event in the stream or null if no new events are currently
   * available.
   *
   * @throws IOException because of network error or edit log
   * corruption. Also possible if JournalNodes are unresponsive in the
   * QJM setting (even one unresponsive JournalNode is enough in rare cases),
   * so catching this exception and retrying at least a few times is
   * recommended.
   * @throws MissingEventsException if we cannot return the next event in the
   * stream because the data for the event (and possibly some subsequent events)
   * has been deleted (generally because this stream is a very large number of
   * events behind the current state of the NameNode). It is safe to continue
   * reading from the stream after this exception is thrown -- the next
   * available event will be returned.
   */
  public Event poll() throws IOException, MissingEventsException {
    // need to keep retrying until the NN sends us the latest committed txid
    if (lastReadTxid == -1) {
      lastReadTxid = namenode.getCurrentEditLogTxid();
      return null;
    }
    if (!it.hasNext()) {
      EventsList el = namenode.getEditsFromTxid(lastReadTxid + 1);
      if (el.getLastTxid() != -1) {
        // we only want to set syncTxid when we were actually able to read some
        // edits on the NN -- otherwise it will seem like edits are being
        // generated faster than we can read them when the problem is really
        // that we are temporarily unable to read edits
        syncTxid = el.getSyncTxid();
        it = el.getEvents().iterator();
        long formerLastReadTxid = lastReadTxid;
        lastReadTxid = el.getLastTxid();
        if (el.getFirstTxid() != formerLastReadTxid + 1) {
          throw new MissingEventsException(formerLastReadTxid + 1,
              el.getFirstTxid());
        }
      } else {
        return null;
      }
    }

    if (it.hasNext()) { // can be empty if el.getLastTxid != -1 but none of the
      // newly seen edit log ops actually got converted to events
      return it.next();
    } else {
      return null;
    }
  }

  /**
   * Return a estimate of how many events behind the NameNode's current state
   * this stream is. Clients should periodically call this method and check if
   * its result is steadily increasing, which indicates that they are falling
   * behind (i.e. events are being generated faster than the client is reading
   * them). If a client falls too far behind events may be deleted before the
   * client can read them.
   * <p/>
   * A return value of -1 indicates that an estimate could not be produced, and
   * should be ignored. The value returned by this method is really only useful
   * when compared to previous or subsequent returned values.
   */
  public long getEventsBehindEstimate() {
    if (syncTxid == 0) {
      return -1;
    } else {
      assert syncTxid >= lastReadTxid;
      // this gives the difference between the last txid we have fetched to the
      // client and syncTxid at the time we last fetched events from the
      // NameNode
      return syncTxid - lastReadTxid;
    }
  }

  /**
   * Returns the next event in the stream, waiting up to the specified amount
   * of time for a new event. Returns null if a new event is not available at
   * the end of the specified amount of time.
   *
   * @param time number of units of the given TimeUnit to wait
   * @param tu the desired TimeUnit
   * @throws IOException see {@link DFSInotifyEventInputStream#poll()}
   * @throws MissingEventsException see
   * {@link DFSInotifyEventInputStream#poll()}
   * @throws InterruptedException if the calling thread is interrupted
   */
  public Event poll(long time, TimeUnit tu) throws IOException,
      InterruptedException, MissingEventsException {
    Future<Event> f = ex.submit(new Callable<Event>() {
      public Event call() throws IOException, MissingEventsException {
        try {
          return take();
        } catch (InterruptedException e) {
          // we've been cancelled
          return null;
        }
      }
    });
    try {
      return f.get(time, tu);
    } catch (TimeoutException e) {
      f.cancel(true);
      return null;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException || e.getCause() instanceof
          Error) {
        throw new UncheckedExecutionException(e.getCause());
      } else {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          throw (MissingEventsException) e.getCause();
        }
      }
    }
  }

  /**
   * Returns the next event in the stream, waiting indefinitely if a new event
   * is not immediately available.
   *
   * @throws IOException see {@link DFSInotifyEventInputStream#poll()}
   * @throws MissingEventsException see
   * {@link DFSInotifyEventInputStream#poll()}
   * @throws InterruptedException if the calling thread is interrupted
   */
  public Event take() throws IOException, InterruptedException,
      MissingEventsException {
    Event next = null;
    long nextWait = INITIAL_WAIT_MS;
    while ((next = poll()) == null) {
      Thread.sleep(nextWait);
      nextWait *= 2;
    }
    return next;
  }
}