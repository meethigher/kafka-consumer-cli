package top.meethigher.kafka.cli.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AutoFlushList<T> {
    private final static Logger log = LoggerFactory.getLogger(AutoFlushList.class);

    private final Timer timer;
    private final ConcurrentLinkedQueue<T> list;
    private final AtomicInteger count;
    private final int maxSize;
    private final long timeoutMillis;
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    public AutoFlushList(int maxSize, long timeoutMillis) {
        this.timer = new Timer();
        this.list = new ConcurrentLinkedQueue<>();
        this.count = new AtomicInteger(0);
        this.maxSize = maxSize;
        this.timeoutMillis = timeoutMillis;

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (count.get() > 0) {
                    log.info("Reach the timeout, execute flush...");
                    flush();
                }
            }
        }, timeoutMillis, timeoutMillis);
    }

    public void add(T element) {
        list.add(element);
        if (count.incrementAndGet() >= maxSize) {
            log.info("Reach the maxSize, execute flush...");
            flush();
        }
    }

    public synchronized void flush() {
        ConcurrentLinkedQueue<T> currentList = new ConcurrentLinkedQueue<>(list);
        list.clear();
        count.set(0);
        executor.submit(() -> onFlush(currentList));
    }

    protected abstract void onFlush(ConcurrentLinkedQueue<T> batch);

}