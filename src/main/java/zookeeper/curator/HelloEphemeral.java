package zookeeper.curator;

import com.sun.jndi.ldap.Connection;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * Created by zzp on 8/1/16.
 */
public class HelloEphemeral {

    static final RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(1000, 3000, 3);

    public static void main(String[] args) {
        Thread p = new Thread(new Provider());
        Thread c = new Thread(new Consumer());
        p.start();
        c.start();
    }

    private static class Provider implements Runnable {
        @Override
        public void run() {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString("127.0.0.1:2181")
                    .retryPolicy(retryPolicy)
                    .connectionTimeoutMs(3000)
                    .sessionTimeoutMs(1500)
                    .build();
            client.start();
            try {
                if (client.checkExists().forPath("/foobar") == null)
                    client.create().forPath("/foobar");
                client.create().withMode(CreateMode.EPHEMERAL).forPath("/foobar/ephe-1");
                Stat stat = client.checkExists().forPath("/foobar/ephe-1");
                System.out.println(Thread.currentThread().getName() + " | Created : " + (stat != null));
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        }
    }

    private static class Consumer implements Runnable {
        @Override
        public void run() {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString("127.0.0.1:2181")
                    .retryPolicy(retryPolicy)
                    .connectionTimeoutMs(3000)
                    .sessionTimeoutMs(1500)
                    .build();
            client.start();
            try {
                Thread.sleep(500);
                while (true) {
                    Stat stat = client.checkExists().forPath("/foobar/ephe-1");
                    System.out.println(Thread.currentThread().getName() + " | Exists : " + (stat != null));
                    if (stat == null)
                        break;
                    else
                        Thread.sleep(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        }
    }
}
