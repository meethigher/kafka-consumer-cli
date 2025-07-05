package top.meethigher.kafka.server.consumer.http.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import top.meethigher.kafka.Record;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RestController
@RequestMapping("/kafka")
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    @Resource
    private JdbcTemplate jdbcTemplate;


    @Value("${jdbc.sync:false}")
    private boolean sync;

    private final String insertSql = "insert into \"record\" " +
            "(\"topic\", \"partition\", \"offset\", \"key\", \"value\", \"timestamp\", \"timestamp_type\") " +
            "VALUES (?, ?, ?, ?, ?, ?, ?);";

    @PostMapping("/records")
    public void records(@RequestBody List<Record> records) {
        if (sync) {
            synchronized (this) {
                jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        Record record = records.get(i);
                        ps.setObject(1, record.getTopic());
                        ps.setObject(2, record.getPartition());
                        ps.setObject(3, record.getOffset());
                        ps.setObject(4, record.getKey());
                        ps.setObject(5, record.getValue());
                        ps.setObject(6, record.getTimestamp());
                        ps.setObject(7, record.getTimestampType());
                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }
                });
                log.info("sync batch insert size {}", records.size());
            }
        } else {
            jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Record record = records.get(i);
                    ps.setObject(1, record.getTopic());
                    ps.setObject(2, record.getPartition());
                    ps.setObject(3, record.getOffset());
                    ps.setObject(4, record.getKey());
                    ps.setObject(5, record.getValue());
                    ps.setObject(6, record.getTimestamp());
                    ps.setObject(7, record.getTimestampType());
                }

                @Override
                public int getBatchSize() {
                    return records.size();
                }
            });
            log.info("batch insert size {}", records.size());
        }

    }
}
