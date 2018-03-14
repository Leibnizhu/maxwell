package io.gitlab.leibnizhu.maxwell.producer;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class HiveConnectionPool {
    private Logger log = LoggerFactory.getLogger(getClass());
	private static final int POOL_INIT_SIZE = 3;
	private static final int POOL_MAX_SIZE = 10;
    private HikariDataSource dataSource;
	private String driver;
	private String jdbcUrl;
	private String username;
	private String password;
	private Map<Connection, Boolean> pool;

    HiveConnectionPool(Properties prop){
    	initKerberos(prop);
		this.pool = new ConcurrentHashMap<>();
		this.driver = prop.getProperty("driver", "org.apache.hive.jdbc.HiveDriver");
		this.jdbcUrl = prop.getProperty("jdbcurl", "jdbc:hive://localhost:10000/");
		this.username = prop.getProperty("user", "");
		this.password = prop.getProperty("password", "");
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			log.error("找不到配置文件中提供的Hive驱动类:{}", driver);
		}
		for (int i = 0; i < POOL_INIT_SIZE; i++) {
			createConnection();
		}
		if(pool.size() == 0){
			log.error("创建连接池失败");
			throw new RuntimeException("创建连接池时尝试"+ POOL_INIT_SIZE+"次均失败");
		}
        /*HikariConfig config = new HikariConfig();
		config.setDriverClassName(driver);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		this.dataSource = new HikariDataSource(config);*/
		log.info("Hive连接池初始化成功!");
    }

	private Connection createConnection() {
		Connection conn = null;
		try {
			conn = (username.length() == 0 || password.trim().length() == 0) ?
					DriverManager.getConnection(jdbcUrl) :
					DriverManager.getConnection(jdbcUrl, username, password);
			pool.put(conn, Boolean.FALSE);
			log.info("连接池加入新连接:{}", conn);
		} catch (SQLException e) {
			log.error("创建连接失败,url:{},用户名:{},密码:{},异常={},SQLState={}", jdbcUrl, username, password, e.getMessage(), e.getSQLState());
		}
		return conn;
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

	Connection getConnection() throws SQLException {
        Connection conn = null;
		for (Map.Entry<Connection, Boolean> entry : pool.entrySet()) {
			Connection tmp = entry.getKey();
			Boolean inUse = entry.getValue();
			try {
				boolean closed = tmp.isClosed();
				log.debug("连接情况:closed={}", closed);
				if (closed) {
					//已关闭或无效的连接,删除之
					pool.remove(tmp);
				} else if (!inUse) {
					//否则有效可用, 且记录中没在使用,则返回
					conn = tmp;
					break;
				}
				//正常使用的跳过
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		//pool中无可用的连接, 且数量未到上限, 则创建新连接
		if (conn == null && pool.size() < POOL_MAX_SIZE) {
			conn = createConnection();
		}
		if (conn != null) {
			pool.put(conn, Boolean.TRUE);
		} else {
			throw new SQLException("连接池中无可用的连接, 且连接数量已达到上限");
		}
        /*try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("从HikariCP连接池获取Hive连接时抛出异常:{}", e.getMessage());
        }*/
        log.info("获取到的连接:{}", conn.toString());
        return conn;
    }

	void back(Connection conn) {
//        if(dataSource != null && !dataSource.isClosed()) {
//            dataSource.close();
//        }
		pool.put(conn, Boolean.FALSE);

	}

	void close() {
		for (Connection conn : pool.keySet()) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
