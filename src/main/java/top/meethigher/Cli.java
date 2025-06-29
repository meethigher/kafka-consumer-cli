package top.meethigher;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;

public class Cli {

    private static final Scanner scanner = new Scanner(System.in);

    //在多线程操作singleton变量时，使用volatile保证线程改变的值立即回写主内存，保证其他线程能拿到最新的值
    private volatile static Logger log;


    public static Logger getLogger() {
        //第一次检查
        if (log == null) {
            //加锁第二次检查
            synchronized (Cli.class) {
                if (log == null) {
                    log = LoggerFactory.getLogger(Cli.class);
                }
            }
        }
        return log;
    }

    private static void loadLogConfig() {
        String logConfigFile = "logback.xml";
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();  // 先重置当前的 Logback 配置
        try {
            // 优先加载外部配置文件，其次加载类路径下配置文件
            String userDirPath = System.getProperty("user.dir").replace("\\", "/");
            File file = new File(userDirPath, logConfigFile);
            URL configFile = null;
            boolean exists = file.exists();
            if (exists) {
                configFile = file.toURI().toURL();
            } else {
                configFile = Cli.class.getClassLoader().getResource(logConfigFile);
            }
            // 获取 classpath 下的配置文件
            if (configFile == null) {
                throw new IllegalArgumentException("Logback configuration file not found: " + logConfigFile);
            }
            // 使用 JoranConfigurator 加载配置
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            configurator.doConfigure(configFile);
            if (exists) {
                getLogger().info("logback configuration loaded successfully: {}", file.getAbsoluteFile());
            } else {
                getLogger().info("logback configuration loaded successfully: {}", logConfigFile);
                try (InputStream is = Cli.class.getClassLoader().getResourceAsStream(logConfigFile)) {
                    Files.copy(is, file.toPath());
                } catch (Exception e) {
                    getLogger().error("create log config file error", e);
                }
                getLogger().info("create log config file {}", file.getAbsoluteFile());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Logback configuration", e);
        }
    }

    public static void main(String[] args) throws Exception {
        // 指定logback使用的环境变量
        System.setProperty("LOG_HOME", "logs");
        loadLogConfig();
        while (true) {
            System.out.println("\nFunction list:");
            System.out.println("1. Query endOffset for all topics");
            System.out.println("2. Subscribe and consume all topics");
            System.out.println("3. Set offset for a specific topic/partition");
            System.out.println("4. One-click to set all partitions to the endOffset.");
            System.out.println("0. Exit");
            System.out.print("\nSelect function: ");

            int choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    queryEndOffset();
                    System.out.println();
                    break;
                case 2:
                    consume();
                    System.out.println();
                    break;
                case 3:
                    setOffset();
                    System.out.println();
                    break;
                case 4:
                    setPartitionsToEndOffset();
                    System.out.println();
                    break;
                case 0:
                    System.out.println("Exiting...");
                    System.out.println();
                    return;
                default:
                    System.out.println("Invalid input. Try again.");
                    System.out.println();
            }
        }
    }

    private static void queryEndOffset() {
        File file = new File(System.getProperty("user.dir"), "kafka.properties");
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
        } catch (Exception e) {
            getLogger().error("Error while reading kafka.properties", e);
            System.exit(1);
        }
        System.out.print("Enter the topic you want to subscribe (eg: topicA,topicB,topicC): ");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<String> list = Arrays.asList(scanner.next().split(","));
            consumer.subscribe(list);
            for (String topic : list) {
                Set<TopicPartition> topicPartitions = new LinkedHashSet<>();
                // 查询topic的所有分区，即使还没有分配给消费者
                List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
                for (PartitionInfo partitionInfo : partitionInfoList) {
                    TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
                    topicPartitions.add(topicPartition);
                }
                // 查询topic分区的endoffset，即使不属于当前消费者。并按照partition进行升序排序
                Map<TopicPartition, Long> sortedMap = new TreeMap<>((o1, o2) -> {
                    // 若比较结果为0，则会判定是同一个值。该问题影响TreeSet和TreeMap
                    return o1.toString().compareTo(o2.toString());
                });
                sortedMap.putAll(consumer.endOffsets(topicPartitions));
                for (TopicPartition partition : sortedMap.keySet()) {
                    System.out.printf("topic=%s, partition=%d, endOffset=%d%n", topic, partition.partition(), sortedMap.get(partition));
                }
            }
        } catch (Exception e) {
            getLogger().error("Error while querying the end offset", e);
            System.exit(1);
        }


    }

    private static void consume() {
        File file = new File(System.getProperty("user.dir"), "kafka.properties");
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
        } catch (Exception e) {
            getLogger().error("Error while reading kafka.properties", e);
            System.exit(1);
        }
        System.out.print("Enter the topic you want to subscribe (eg: topicA,topicB,topicC): ");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {
            List<String> list = Arrays.asList(scanner.next().split(","));
            consumer.subscribe(list);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            getLogger().error("Error while consuming", e);
            System.exit(1);
        }
    }

    private static void setOffset() {
        File file = new File(System.getProperty("user.dir"), "kafka.properties");
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
        } catch (Exception e) {
            getLogger().error("Error while reading kafka.properties", e);
            System.exit(1);
        }
        System.out.print("Enter whether you want to set the endOffset in bulk (true/false): ");
        boolean inBulk = scanner.nextBoolean();
        System.out.print("Enter the topic you want to subscribe (eg: topicA,topicB,topicC): ");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {
            List<String> list = Arrays.asList(scanner.next().split(","));
            consumer.subscribe(list);

            long offset = 0;
            if (inBulk) {
                System.out.print("Enter the offset you want to set: ");
                offset = scanner.nextLong();
            }
            // 先poll一次，触发分区分配
            // 获取分配当前消费者的topic分区。因为只能设置分配给自己的topic分区
            Set<TopicPartition> assignment = Collections.emptySet();
            while (assignment.isEmpty()) {
                consumer.poll(Duration.ofMillis(1000));
                assignment = consumer.assignment();
            }
            // 对每个分区调用seek，从指定offset开始消费。并按照partition升序进行设置
            Set<TopicPartition> sortedAssignment = new TreeSet<>((o1, o2) -> {
                // 若比较结果为0，则会判定是同一个值。该问题影响TreeSet和TreeMap
                return o1.toString().compareTo(o2.toString());
            });
            sortedAssignment.addAll(assignment);
            for (TopicPartition tp : sortedAssignment) {
                if (!inBulk) {
                    System.out.print("Enter the offset you want to set for partition " + tp.partition() + ": ");
                    offset = scanner.nextLong();
                }
                consumer.seek(tp, offset);
                consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(offset)));
                System.out.printf("Seek topic %s partition %d to offset %d%n", tp.topic(), tp.partition(), offset);
            }
        } catch (Exception e) {
            getLogger().error("Error while setting offset", e);
            System.exit(1);
        }
    }

    private static void setPartitionsToEndOffset() {
        File file = new File(System.getProperty("user.dir"), "kafka.properties");
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
        } catch (Exception e) {
            getLogger().error("Error while reading kafka.properties", e);
            System.exit(1);
        }
        System.out.print("Enter the topic you want to subscribe (eg: topicA,topicB,topicC): ");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<String> list = Arrays.asList(scanner.next().split(","));
            consumer.subscribe(list);
            // 先poll一次，触发分区分配
            // 获取分配当前消费者的topic分区。因为只能设置分配给自己的topic分区
            Set<TopicPartition> assignment = Collections.emptySet();
            while (assignment.isEmpty()) {
                consumer.poll(Duration.ofMillis(1000));
                assignment = consumer.assignment();
            }
            // 查询topic分区的endoffset，即使不属于当前消费者。并按照partition进行升序排序
            Map<TopicPartition, Long> sortedMap = new TreeMap<>((o1, o2) -> {
                // 若比较结果为0，则会判定是同一个值。该问题影响TreeSet和TreeMap
                return o1.toString().compareTo(o2.toString());
            });
            sortedMap.putAll(consumer.endOffsets(assignment));
            for (TopicPartition tp : sortedMap.keySet()) {
                Long offset = sortedMap.get(tp);
                consumer.seek(tp, offset);
                consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(offset)));
                System.out.printf("Seek topic %s partition %d to offset %d%n", tp.topic(), tp.partition(), offset);
            }
        } catch (Exception e) {
            getLogger().error("Error while querying the end offset", e);
            System.exit(1);
        }

    }


}
