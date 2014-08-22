package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InotifyPerformanceRig {

  public boolean takeEvent(DFSInotifyEventInputStream eis) throws
  IOException, InterruptedException, MissingEventsException {
    return eis.poll(1, TimeUnit.SECONDS) != null;
    // URL u = new URL("http://www.google.com");
    // u.getContent();
    // return true;
  }

  @Test
  public void test() throws IOException, InterruptedException {
    final int numInotifyClients = 1;
    final int numOps = 10000;

    Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).build();

    try {
      cluster.getDfsCluster().waitActive();
      cluster.getDfsCluster().transitionToActive(0);
      cluster.getDfsCluster().shutdownNameNode(1); // kill the Standby
      final DFSClient client = new DFSClient(cluster.getDfsCluster()
          .getNameNode(0).getNameNodeAddress(), conf);

      final CountDownLatch cl = new CountDownLatch(numInotifyClients);

      client.mkdirs("/test", null, false);
      ExecutorService ex = Executors.newCachedThreadPool();
      ex.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cl.await();
          } catch (InterruptedException e) {
            // won't happen
          }
          long start = Time.monotonicNow();
          for (int i = 0; i < numOps; i++) {
            try {
              client.setOwner("/test", "james", "james");
            } catch (Exception e) {
              System.out.println("Failed... " + e + ", " + e.getMessage());
            }
          }
          System.out.println("Ops per ms: " + (double) numOps / (Time.monotonicNow() - start));
        }
      });

      for (int i = 0; i < numInotifyClients; i++) {
        ex.submit(new Runnable() {
          @Override
          public void run() {
            try {
              DFSInotifyEventInputStream eis = client.getInotifyEventStream();
              cl.countDown();
              int eventCount = 0;
              while (eventCount < numOps) {
                // if (takeEvent(eis)) {
                //  eventCount++;
                // }
                // takeEvent(eis);
                // eventCount += 10;
                eventCount += eis.dummyPoll();
              }
            } catch (Exception e) {
              System.out.println("Failed... " + e + ", " + e.getMessage());
            }
          }
        });
      }

      ex.shutdown();
      ex.awaitTermination(5, TimeUnit.MINUTES);
      System.out.println("Done...");
      // single thread performing a bunch of directory creations
      // numInotifyClients threads reading all of the edits
      // introduce more reasonable network delays (or should we -- in theory
      // with tons of clients requests will be received continuously at the NN)
    } finally {
      cluster.shutdown();
    }
  }
}
