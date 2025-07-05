package top.meethigher.kafka.cli.tools;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class FlushQueueTest {


    private static final Logger log = LoggerFactory.getLogger(FlushQueueTest.class);

    @Test
    public void add() {
        try (FlushQueue<String> cache = new FlushQueue<String>(10, 5000) {
            @Override
            protected void onFlush(ConcurrentLinkedQueue<String> batch) throws Exception {
                log.info("onFlush start");
                Thread.sleep(15000);
                log.info("{}", batch);
                log.info("onFlush end");
            }
        }) {
            for (int i = 0; i < 9; i++) {
                cache.add(String.valueOf(i));
            }
            List<Thread> list = new ArrayList<>();
            for (int i = 9; i < 20; i++) {
                final int finalI = i;
                list.add(new Thread(() -> cache.add(String.valueOf(finalI))));
            }
            for (Thread thread : list) {
                thread.start();
            }

            TimeUnit.SECONDS.sleep(1);
//            LockSupport.park();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void flush() {
        try (FlushQueue<String> cache = new FlushQueue<String>(10, 5000) {

            @Override
            protected void onFlush(ConcurrentLinkedQueue<String> batch) throws Exception {
                log.info("{}", batch);
            }
        }) {
            new Thread(() -> {
                while (true) {
                    cache.flush();
                    try {
                        Thread.sleep(2000);
                    } catch (Exception e) {
                    }
                }
            }).start();
            for (int i = 0; i < 100; i++) {
                cache.add(String.valueOf(i));
            }

            LockSupport.park();
        } catch (Exception e) {
        }
    }
}