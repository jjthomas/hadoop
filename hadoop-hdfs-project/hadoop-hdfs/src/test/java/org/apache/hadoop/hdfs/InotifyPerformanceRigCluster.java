package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InotifyPerformanceRigCluster {

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
    System.setProperty("log4j.configuration", "file:///home/james/hdfs-conf/log4j.properties");
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/james/hdfs-conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/james/hdfs-conf/core-site.xml"));
    URI uri = FileSystem.getDefaultUri(conf);
    final DistributedFileSystem dfs =
        (DistributedFileSystem) FileSystem.get(uri, conf);

    final CountDownLatch cl = new CountDownLatch(numInotifyClients);

    dfs.mkdirs(new Path("/test"));

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
            dfs.setOwner(new Path("/test"), "james", "james");
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
            DFSInotifyEventInputStream eis = dfs.getInotifyEventStream();
            cl.countDown();
            int eventCount = 0;
            while (eventCount < numOps) {
              if (takeEvent(eis)) {
                eventCount++;
              }
              // takeEvent(eis);
              // eventCount += 10;
              // eventCount += eis.dummyPoll();
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
  }
}
