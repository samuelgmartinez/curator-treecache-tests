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

    private static final String PATH = "/test";
    private static final int ITERATIONS = 50;

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
        log.info("TESTING NON existent root treecache");
        testListener(PATH, fw);
    }

    @Test
    public void testListenerExistentRoot() throws Exception {
        log.info("TESTING existent root treecache");
        fw.create().forPath(PATH);
        testListener(PATH, fw);
    }

    @Test
    public void testListenerNonExistentRootLoop() throws Exception {
        log.info("TESTING NON existent root treecache loop");

        for(int i = 0 ; i<ITERATIONS ; i++) {
            Thread.sleep(100);
            CuratorFramework customFW = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryNTimes(2, 100));
            customFW.start();
            customFW.blockUntilConnected();

            testListener(PATH, customFW);

            customFW.close();

        }
    }

    private void testListener(String path, CuratorFramework fw) throws Exception {
        TreeCache cache = new TreeCache(fw, path);
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
