package zookeeper.curator.hello;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * Created by zzp on 8/2/16.
 */
public class HelloListener {

    static final RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(1000, 3000, 3);
    public static final String FOOBAR_EPHE1 = "/foobar/ephe-1";

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
                client.create().withMode(CreateMode.EPHEMERAL).forPath(FOOBAR_EPHE1);
                Stat stat = client.checkExists().forPath(FOOBAR_EPHE1);
                System.out.println(Thread.currentThread().getName() + " | Created : " + (stat != null));
                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println(Thread.currentThread().getName() + " | Close connection");
                client.close();
            }
        }
    }

    private static class Consumer implements Runnable {

        CountDownLatch countDownLatch = new CountDownLatch(1);
        @Override
        public void run() {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString("127.0.0.1:2181")
                    .retryPolicy(retryPolicy)
                    .connectionTimeoutMs(3000)
                    .sessionTimeoutMs(1500)
                    .build();
            client.start();
            CuratorListener listener = new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    System.out.println("EventReceived: " + eventToString(event));
                }
            };
            client.getCuratorListenable().addListener(listener);
            try {
                Thread.sleep(500);
                Stat stat = client.checkExists().forPath(FOOBAR_EPHE1);
                System.out.println(Thread.currentThread().getName() + " | Exists : " + (stat != null));
                if (stat != null) {
                    client.getChildren().watched().forPath("/foobar");

                    // setting sync
                    client.setData().forPath(FOOBAR_EPHE1, "abc".getBytes("UTF-8"));
                    Thread.sleep(100);

                    // setting async
                    client.setData().inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                            System.out.println("ProcessResult: " + eventToString(event));
                            countDownLatch.countDown();
                        }
                    }).forPath(FOOBAR_EPHE1, "abc".getBytes("UTF-8"));
                }
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        }

        private String eventToString(CuratorEvent event) {
            StringBuffer sb = new StringBuffer("{");
            sb.append("type:").append(event.getType()).append(", ");
            sb.append("name:").append(event.getName()).append(", ");
            sb.append("result:").append(event.getResultCode()).append(", ");
            sb.append("path:").append(event.getPath()).append(", ");
            //sb.append("context:").append(event.getContext()).append(", ");
            //sb.append("stat:").append(event.getStat()).append(", ");
            //sb.append("data:").append(new String(event.getData()));
            sb.append("}");
            return sb.toString();
        }
    }

}
