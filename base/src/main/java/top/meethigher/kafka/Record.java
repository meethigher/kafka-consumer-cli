package top.meethigher.kafka;

public class Record {
    private String topic;
    private Integer partition;
    private Long offset;
    private String key;
    private String value;
    private Long timestamp;
    private String timestampType;

    public Record(String topic, Integer partition, Long offset, String key, String value, Long timestamp, String timestampType) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
    }

    public Record() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestampType() {
        return timestampType;
    }

    public void setTimestampType(String timestampType) {
        this.timestampType = timestampType;
    }

    @Override
    public String toString() {
        return String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s, timestamp=%d, timestampType=%s",
                this.topic,
                this.partition,
                this.offset,
                this.key,
                this.value,
                this.timestamp,
                this.timestampType);
    }
}
