package zookeeper.curator.hello;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;

import java.util.concurrent.CountDownLatch;

/**
 * Created by zzp on 8/8/16.
 */
public class HelloNodeCache {

    static final RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(1000, 3000, 3);
    public static final String FOOBAR_CACHE = "/foobar/cache";

    static CountDownLatch countDownLatch = new CountDownLatch(1);

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
                String path = ZKPaths.makePath(FOOBAR_CACHE, "first");
                System.out.println(Thread.currentThread().getName() + " | create node 'first'");
                client.create().creatingParentsIfNeeded().forPath(path, "hello".getBytes());
                Thread.sleep(1500);

                System.out.println(Thread.currentThread().getName() + " | update node 'first'");
                client.setData().forPath(path, "world".getBytes());
                Thread.sleep(1500);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println(Thread.currentThread().getName() + " | Close connection");
                CloseableUtils.closeQuietly(client);
                countDownLatch.countDown();
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
            } catch (InterruptedException e) {
            }
            String first = ZKPaths.makePath(FOOBAR_CACHE, "first");
            NodeCache cache = new NodeCache(client, first);
            NodeCacheListener listener = new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    String path = cache.getCurrentData().getPath();
                    String data = new String(cache.getCurrentData().getData());
                    System.out.println("Node '" + path + "' changed, " + data);
                }
            };
            cache.getListenable().addListener(listener);
            try {
                cache.start();

                countDownLatch.await();

                client.delete().deletingChildrenIfNeeded().forPath(FOOBAR_CACHE);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                CloseableUtils.closeQuietly(cache);
                CloseableUtils.closeQuietly(client);
            }
        }
    }
}
