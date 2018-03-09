package io.gitlab.leibnizhu.maxwell.producer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HiveConnection {
    private Logger log = LoggerFactory.getLogger(getClass());
    private HikariDataSource dataSource;

    HiveConnection(Properties prop){
        HikariConfig config = new HikariConfig();
		config.setDriverClassName(prop.getProperty("driver", "org.apache.hive.jdbc.HiveDriver"));
        config.setJdbcUrl(prop.getProperty("jdbcurl", "jdbc:mysql://localhost:3306/"));
        config.setUsername(prop.getProperty("user", ""));
        config.setPassword(prop.getProperty("password", ""));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        this.dataSource = new HikariDataSource(config);
        log.info("HikariCP Hive连接池初始化成功！");
    }

    Connection getConnection(){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("从HikariCP连接池获取Hive连接时抛出异常", e);
        }
        return conn;
    }

    void close(){
        if(dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}
