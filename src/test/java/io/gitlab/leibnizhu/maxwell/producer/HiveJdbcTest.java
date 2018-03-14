package io.gitlab.leibnizhu.maxwell.producer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class HiveJdbcTest {
	private Logger log = LoggerFactory.getLogger(getClass());

	private Properties prop;
	@Before
	public void init(){
		prop = new Properties();
		prop.setProperty("factory", "io.gitlab.leibnizhu.maxwell.producer.HiveProducerFactory");
		prop.setProperty("strict", "database");
		prop.setProperty("rule", "test.user->default.testuser,M,id;AAaa.Bbb->Ccc.Ddd,A;AAaa2.Bbb2->Ccc2.Ddd2,M;AAaa2->Ccc2,M;AAaa3.Bbb3->Ccc3.Ddd3;AAaa4.Bbb4->Ccc4.Ddd4,XX;AAaa3->Ccc3;AAaa4.->Ccc4.");
		prop.setProperty("jdbcurl", "jdbc:hive2://cdh1:10000/default;principal=hive/cdh1@TURINGDI.COM");
		prop.setProperty("driver", "com.cloudera.hive.jdbc4.HS2Driver");
		prop.setProperty("kerberos.on", "true");
		prop.setProperty("kerberos.krb5conf", "/Users/leibnizhu/krb5.conf");
		prop.setProperty("kerberos.user", "hive");
		prop.setProperty("kerberos.keytab", "/Users/leibnizhu/hive.keytab");
	}

	@Test
	public void kerberosInsertTest() {
		String hiveHQL = "INSERT INTO default.testuser (id,name,age,fm) VALUES ('78','fff','3','jyd')";
		HiveConnectionPool pool = new HiveConnectionPool(prop);
		Connection conn = null;
		Statement stat = null;
		try {
			conn = pool.getConnection();
			stat = conn.createStatement();
			int affected = stat.executeUpdate(hiveHQL);
			log.info("执行HQL完毕, 影响了{}行记录", affected);
		} catch (SQLException e) {
			log.error("执行Hive HQL语句时抛出异常:{}", e.getMessage());
		} finally {
			closeAll(stat, conn);
		}
	}

	@Test
	public void kerberosSelectTest() {
		String hiveHQL = "select * from hivetest";
		HiveConnectionPool pool = new HiveConnectionPool(prop);
		Connection conn = null;
		PreparedStatement stat = null;
		try {
			conn = pool.getConnection();
			stat = conn.prepareStatement(hiveHQL);
			ResultSet rs = stat.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getInt(1) + " -> " + rs.getString(2));
			}
		} catch (SQLException e) {
			log.error("执行Hive HQL语句时抛出异常:{}", e.getMessage());
		} finally {
			closeAll(stat, conn);
		}
	}

	@Test
	public void jdbcTest() throws Exception {
		System.setProperty("java.security.krb5.conf", "/Users/leibnizhu/krb5.conf"); //krb5.conf本地路径
		Configuration config = new Configuration();
		config.set("hadoop.security.authentication", "kerberos"); //必须有
		UserGroupInformation.setConfiguration(config);
		UserGroupInformation.loginUserFromKeytab("hbase", "/Users/leibnizhu/hbase.keytab"); //Kerberos用户名, keytab本地路径
		Class.forName("org.apache.hive.jdbc.HiveDriver");
//		Class.forName("com.cloudera.hive.jdbc4.HS2Driver");
		// 注意：这里的principal是固定不变的，其指的hive服务所对应的principal,而不是用户所对应的principal
		Connection conn = DriverManager.getConnection("jdbc:hive2://cdh1:10000/default;principal=hive/cdh1@TURINGDI.COM");

//		HikariConfig hikariConfig = new HikariConfig();
////		hikariConfig.setDriverClassName( "org.apache.hive.jdbc.HiveDriver");
////		hikariConfig.setDriverClassName( "com.cloudera.hive.jdbc4.HS2Driver");
//		hikariConfig.setJdbcUrl("jdbc:hive2://cdh1:10000/default;principal=hive/cdh1@TURINGDI.COM");
//		hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
//		hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
//		hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
//		HikariDataSource dataSource = new HikariDataSource(hikariConfig);
//		Connection conn = dataSource.getConnection();

		PreparedStatement stat= conn.prepareStatement("select * from hivetest");
		ResultSet rs = stat.executeQuery();
		while(rs.next()){
			System.out.println(rs.getInt(1)+" -> "+rs.getString(2));
		}
	}

	private void closeAll(AutoCloseable... c) {
		for (AutoCloseable src : c) {
			try {
				if (src != null) {
					src.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
