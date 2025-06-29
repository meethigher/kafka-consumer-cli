package top.meethigher.kafka.server.consumer.http;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@SpringBootApplication
public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        SpringApplication.run(Server.class, args);
    }

    @Value("${jdbc.url}")
    private String url;
    @Value(("${jdbc.username}"))
    private String username;
    @Value(("${jdbc.password}"))
    private String password;

    @Bean
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        return new HikariDataSource(config);
    }

    @Bean
    public JdbcTemplate jdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "drop table if exists record;\n" +
                "create table \"record\" (\n" +
                "\t\"topic\" varchar,\n" +
                "\t\"partition\" int4,\n" +
                "\t\"offset\" int8,\n" +
                "\t\"key\" varchar,\n" +
                "\t\"value\" text,\n" +
                "\t\"timestamp\" int8,\n" +
                "\t\"timestamp_type\" varchar\n" +
                ");";
        jdbcTemplate.update(sql);
        log.info("execute sql\n{}", sql);
        return jdbcTemplate;
    }

}
