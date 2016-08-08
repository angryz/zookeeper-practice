package zookeeper.curator.hello;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.CountDownLatch;

/**
 * Created by zzp on 8/7/16.
 */
public class HelloPathChildrenCache {

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

                path = ZKPaths.makePath(path, "second");
                System.out.println(Thread.currentThread().getName() + " | create node 'second'");
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, "222".getBytes());
                Thread.sleep(1500);

                System.out.println(Thread.currentThread().getName() + " | update node 'second'");
                client.setData().forPath(path, "666".getBytes());
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

            String first = ZKPaths.makePath(FOOBAR_CACHE, "first");
            PathChildrenCache cache = new PathChildrenCache(client, FOOBAR_CACHE, true);
            PathChildrenCacheListener listener = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    System.out.println(" - Event: " + event.toString());
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            System.out.println("  - Node added: " + event.getData().getPath());
                            System.out.println("  - Node value: " + new String(event.getData().getData()));
                            if (event.getData().getPath().equals(first)) {
                                PathChildrenCache cache1 = new PathChildrenCache(client, first, true);
                                cache1.getListenable().addListener(this);
                                cache1.start();
                            }
                            break;
                        case CHILD_UPDATED:
                            System.out.println("  - Node updated: " + event.getData().getPath());
                            System.out.println("  - Node value: " + new String(event.getData().getData()));
                            break;
                        case CHILD_REMOVED:
                            System.out.println("  - Node removed: " + event.getData().getPath());
                            break;
                    }
                }
            };
            cache.getListenable().addListener(listener);

            try {
                cache.start();

                Thread.sleep(3000);
                System.out.println(Thread.currentThread().getName() + " | " + new String(cache.getCurrentData(first).getData()));

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
