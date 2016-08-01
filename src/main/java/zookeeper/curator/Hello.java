package zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;

/**
 * Created by zzp on 8/1/16.
 */
public class Hello {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(2000, 10000, 3);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(1500)
                .sessionTimeoutMs(2000)
                .build();
        curatorFramework.start();
        try {
            curatorFramework.create().creatingParentsIfNeeded().forPath("/foo/bar", "world!".getBytes(Charset.forName("UTF-8")));
            byte[] bs = curatorFramework.getData().forPath("/foo/bar");
            System.out.println("hello " + new String(bs));
            curatorFramework.delete().deletingChildrenIfNeeded().forPath("/foo");
            Stat stat = curatorFramework.checkExists().forPath("/foo/bar");
            System.out.println("/foo/bar exists : " + (stat != null));
            stat = curatorFramework.checkExists().forPath("/foo");
            System.out.println("/foo exists : " + (stat != null));
        } finally {
            curatorFramework.close();
        }
    }
}
