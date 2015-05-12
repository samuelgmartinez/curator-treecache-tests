import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by samuelg on 12/05/15.
 */
public class TreeCacheTest {

    private static final Logger log = LoggerFactory.getLogger(TreeCacheTest.class);

    private TestingServer zkServer;
    private CuratorFramework fw;

    @Before
    public void setup() throws Exception {
        log.info("Setting the testsuite up...");

        zkServer = new TestingServer();
        zkServer.start();

        fw = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryNTimes(2, 100));
        fw.start();
        fw.blockUntilConnected();
    }

    @After
    public void tearDown() throws Exception {
        log.info("Tear the testsuite down...");
        fw.close();
        zkServer.close();
    }

    @Test
    public void testListenerNonExistentRoot() throws Exception {
        TreeCache cache = new TreeCache(fw, "/test");
        CountDownLatch latch = new CountDownLatch(1);

        cache.getListenable().addListener((client, event) -> {
            log.info("event received {}", event.getType().name());
            if (event.getType() == TreeCacheEvent.Type.INITIALIZED) {
                latch.countDown();
            }
        });

        cache.start();

        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));

        cache.close();
    }

    @Test
    public void testListenerExistentRoot() throws Exception {
        fw.create().forPath("/test");

        TreeCache cache = new TreeCache(fw, "/test");
        CountDownLatch latch = new CountDownLatch(1);

        cache.getListenable().addListener((client, event) -> {
            log.info("event received {}", event.getType().name());
            if (event.getType() == TreeCacheEvent.Type.INITIALIZED) {
                latch.countDown();
            }
        });

        cache.start();

        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        cache.close();
    }

}
