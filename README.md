# kafka-consumer-cli

kafka-consumer 的 cli 工具。支持

1. 查看topic分区的endOffset
2. 消费消息。本地消费与HTTP远程消费
3. 自定义提交已分配topic分区的offset

交互内容在控制台输出。
日志在程序根目录的logs文件夹。

```sh
java -jar consumer-cli.jar

Function list:
1. Query endOffset for all topics
2. Subscribe and consume all topics
3. Set offset for a specific topic/partition
4. One-click to set all partitions to the endOffset.
0. Exit

Select function: 0
Exiting...
```

