package io.gitlab.leibnizhu.maxwell.producer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HiveConnectionPool {
    private Logger log = LoggerFactory.getLogger(getClass());
    private HikariDataSource dataSource;

    HiveConnectionPool(Properties prop){
    	initKerberos(prop);
        HikariConfig config = new HikariConfig();
		config.setDriverClassName(prop.getProperty("driver", "org.apache.hive.jdbc.HiveDriver"));
        config.setJdbcUrl(prop.getProperty("jdbcurl", "jdbc:hive://localhost:10000/"));
        config.setUsername(prop.getProperty("user", ""));
        config.setPassword(prop.getProperty("password", ""));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		this.dataSource = new HikariDataSource(config);
        log.info("HikariCP Hive连接池初始化成功!");
    }

	private void initKerberos(Properties prop) {
    	Boolean useKerberos = Boolean.valueOf(prop.getProperty("kerberos.on", "false"));
    	if(!useKerberos){
    		log.debug("根据配置文件, 无需开启Kerberos支持");
    		return;
		}
		KerberosConfig config = new KerberosConfig(prop);
		System.setProperty("java.security.krb5.conf", config.krb5ConfPath); //krb5.conf本地路径
		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("hadoop.security.authentication", "kerberos"); //必须有
		UserGroupInformation.setConfiguration(hadoopConfig);
		log.info("使用Kerberos用户{}及keytab(路径:{})进行身份认证, 配置文件为:{}", config.kerberosUser, config.kerberosKeytabPath, config.krb5ConfPath);
		try {
			UserGroupInformation.loginUserFromKeytab(config.kerberosUser, config.kerberosKeytabPath);
		} catch (IOException e) {
			log.error("Kerberos身份认证失败, 抛出异常:{}", e.getMessage());
		}
		log.info("Kerberos身份认证成功");
	}

	Connection getConnection(){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("从HikariCP连接池获取Hive连接时抛出异常:{}", e.getMessage());
        }
        return conn;
    }

    void close(){
        if(dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}
