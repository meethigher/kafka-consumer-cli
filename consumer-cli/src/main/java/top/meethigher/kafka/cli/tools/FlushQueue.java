package top.meethigher.kafka.cli.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程安全的自动刷新队列。
 * <p>
 * 支持根据缓存最大值和时效性来触发刷新操作。
 */
public abstract class FlushQueue<T> implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(FlushQueue.class);
    protected static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    protected final Timer timer;

    protected ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    protected final AtomicInteger count;
    protected final int maxSize;
    protected final long timeoutMillis;
    protected final ExecutorService executor;
    protected final AtomicBoolean flushing = new AtomicBoolean(false);
    protected final ReentrantLock lock = new ReentrantLock();
    protected final String name;


    /**
     * 触发线程安全的flush。注意该操作仅允许内部调用
     */
    protected void flush() {
        // 以下三步的时间间隙中，会出现并发add导致丢失数据问题。使用ReentrantLock解决
        lock.lock();
        ConcurrentLinkedQueue<T> flushQueue;
        try {
            flushQueue = queue;
            queue = new ConcurrentLinkedQueue<>();
            count.set(0);
        } finally {
            lock.unlock();
        }
        executor.submit(() -> {
            try {
                onFlush(flushQueue);
            } catch (Exception e) {
                log.error("{} failed to execute onFlush", name, e);
            }
        });
        flushing.set(false);
    }


    public FlushQueue(String name, int maxSize, long timeoutMillis, ExecutorService executor) {
        this.timer = new Timer();
        this.count = new AtomicInteger(0);
        this.maxSize = maxSize;
        this.timeoutMillis = timeoutMillis;
        this.executor = executor;
        this.name = name;

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    boolean tried = tryFlush();
                    if (tried) {
                        log.info("{} reach the timeout {} ms, execute flush...", name, timeoutMillis);
                    }
                } catch (Exception e) {
                    log.error("{} reach the timeout {} ms, execute error", name, timeoutMillis, e);
                }
            }
        }, timeoutMillis, timeoutMillis);
        log.info("{} created", name);
    }

    public FlushQueue(int maxSize, long timeoutMillis) {
        this(generateName(), maxSize, timeoutMillis, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2));
    }

    public void add(T element) {
        boolean doFlush = false;
        lock.lock();
        try {
            queue.add(element);
            // 通过CAS保证在多线程执行时，list满了之后，不会被多线程重复flush
            if (count.incrementAndGet() >= maxSize && flushing.compareAndSet(false, true)) {
                log.info("{} reach the maxSize {}, execute flush...", name, maxSize);
                doFlush = true;
            }
        } finally {
            lock.unlock();
        }
        if (doFlush) {
            flush();
        }
    }

    public boolean tryFlush() {
        boolean doFlush = false;
        lock.lock();
        try {
            // 通过CAS保证在多线程执行时，list满了之后，不会被多线程重复flush
            if (count.get() > 0 && flushing.compareAndSet(false, true)) {
                doFlush = true;
            }
        } finally {
            lock.unlock();
        }
        if (doFlush) {
            flush();
        }
        return doFlush;
    }

    @Override
    public void close() throws Exception {
        log.info("{} closing", name);
        timer.cancel();
        tryFlush();
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
        log.info("{} closed", name);
    }

    protected static String generateName() {
        final String prefix = FlushQueue.class.getSimpleName() + "-";
        try {
            // 池号对于虚拟机来说是全局的，以避免在类加载器范围的环境中池号重叠
            synchronized (System.getProperties()) {
                final String next = String.valueOf(Integer.getInteger(FlushQueue.class.getName() + ".name", 0) + 1);
                System.setProperty(FlushQueue.class.getName() + ".name", next);
                return prefix + next;
            }
        } catch (Exception e) {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            final StringBuilder sb = new StringBuilder(prefix);
            for (int i = 0; i < 4; i++) {
                sb.append(ID_CHARACTERS[random.nextInt(ID_CHARACTERS.length)]);
            }
            return sb.toString();
        }
    }

    protected abstract void onFlush(ConcurrentLinkedQueue<T> batch) throws Exception;

}