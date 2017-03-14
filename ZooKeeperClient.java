# ZookeeperTest


package test;  
  
import org.apache.zookeeper.WatchedEvent;  
import org.apache.zookeeper.Watcher;  
import org.apache.zookeeper.ZooKeeper;  
  
import java.io.IOException;  
import java.util.concurrent.CountDownLatch;  
import java.util.concurrent.TimeUnit;  
  

public class ZooKeeperClient {  
  
    private static String connectionString = "xjstest01:2181";  
    private static int sessionTimeout = 10000;  
  
  
    public static ZooKeeper getInstance() throws IOException, InterruptedException {  
        final CountDownLatch connectedSignal = new CountDownLatch(1);  
        ZooKeeper zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {  
            @Override  
            public void process(WatchedEvent event) {  
                if (event.getState() == Event.KeeperState.SyncConnected) {  
                    connectedSignal.countDown();  
                }  
            }  
        });  
        connectedSignal.await(sessionTimeout, TimeUnit.MILLISECONDS);  
        return zk;  
    }  
  
    public static int getSessionTimeout() {  
        return sessionTimeout;  
    }  
  
    public static void setSessionTimeout(int sessionTimeout) {  
        ZooKeeperClient.sessionTimeout = sessionTimeout;  
    }  
  
}  
