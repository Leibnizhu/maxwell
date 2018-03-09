package io.gitlab.leibnizhu.maxwell.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class HQLExecuteThread implements Runnable{
	private Logger log = LoggerFactory.getLogger(getClass());

	private HiveConnection connPool;
	private String hiveHQL;

	HQLExecuteThread(HiveConnection connPool, String hiveHQL) {
		this.connPool = connPool;
		this.hiveHQL = hiveHQL;
	}

	@Override
	public void run() {
		if (hiveHQL == null || hiveHQL.trim().length() == 0) {
			return;
		}
		Connection conn = null;
		Statement stat = null;
		try {
			log.info("准备执行HQL: " + hiveHQL);
			conn = connPool.getConnection();
			stat = conn.createStatement();
			int affected = stat.executeUpdate(hiveHQL);
			log.info("执行HQL完毕, 影响了{}行记录", affected);
		} catch (SQLException e) {
			log.error("执行Hive HQL语句时抛出异常:{}", e.getMessage());
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					log.error("关闭Hive Statement时抛出异常:{}", e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error("关闭Hive连接时抛出异常:{}", e.getMessage());
				}
			}
		}
	}
}
