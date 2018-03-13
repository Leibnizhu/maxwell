package io.gitlab.leibnizhu.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.gitlab.leibnizhu.maxwell.producer.HiveConfig.*;

public class HiveProducer extends AbstractProducer implements StoppableTask {
	private Logger log = LoggerFactory.getLogger(getClass());
	private HiveConfig config;
	private HiveConnectionPool connPool;
	private ExecutorService threadPool;

	HiveProducer(MaxwellContext context) {
		super(context);
		log.info("===============HiveProducer开始初始化==============");
		Properties prop = context.getConfig().customProducerProperties;
		this.config = new HiveConfig(prop);
		this.connPool = new HiveConnectionPool(prop);
		this.threadPool = Executors.newFixedThreadPool(10);
		log.info("===============HiveProducer初始化完毕==============");
	}

	@Override
	public void push(RowMap rowMap) {
		String type = rowMap.getRowType();
		String mysqlDatabase = rowMap.getDatabase();
		String mysqlTable = rowMap.getTable();
		log.debug("接收到MySQL变更通知,类型={},数据库={},表={}", type, mysqlDatabase, mysqlTable);
		if (!type.equals("insert") && !type.equals("update")) {
			log.debug("不支持的数据库操作:{}", type);
			return;
		}
		HiveTable hiveTable = matchHiveTable(mysqlDatabase, mysqlTable);
		String hiveTableFullName = hiveTable.fullTable;
		if (hiveTableFullName == null || hiveTableFullName.trim().length() == 0) {
			log.debug("暂未找到MySQL表{}对应的Hive表, 忽略本次MySQL操作", mysqlDatabase + "." + mysqlTable);
			return;
		}
		/*
		 *        |    APPEND   | MODIFY  |
		 * -------|-------------|---------|
		 * insert |   support   | support |
		 * update | not-support | support |
		 */
		if (hiveTable.mode == null || (hiveTable.mode.equals(HiveMode.APPEND) && type.equals("update"))) {
			log.debug("Hive表{}在规则中不支持更新操作, 忽略MySQL相应表{}的update操作.", hiveTableFullName, mysqlDatabase + "." + mysqlTable);
			return;
		}
		String hiveHQL = makeHQL(hiveTable, rowMap.getData(), type);
		//放入线程池 等待被执行, 不要阻塞进程
		threadPool.execute(new HQLExecuteThread(connPool, hiveHQL));

	}

	private HiveTable matchHiveTable(String mysqlDatabase, String mysqlTable) {
		String hiveDatabase = null;
		String hiveTable = null;
		HiveMode mode = null;
		String primaryKey = null;
		if (config.containTable(mysqlDatabase, mysqlTable)) {
			//1. 表匹配上了
			HiveTable hiveTableResult = config.hiveTable(mysqlDatabase, mysqlTable);
			hiveTable = hiveTableResult.table;
			hiveDatabase = hiveTableResult.database;
			mode = hiveTableResult.mode;
			primaryKey = hiveTableResult.primaryKey;
		} else if (!config.strict().equals(Strict.TABLE) && config.containDatabase(mysqlDatabase)) {
			//2. 只匹配表映射, 但又没有对应表映射, 则不继续匹配
			//3. 库名匹配上了
			HiveDatabase hiveDatabaseResult = config.hiveDatabase(mysqlDatabase);
			hiveDatabase = hiveDatabaseResult.database;
			hiveTable = mysqlTable;
			mode = hiveDatabaseResult.mode;
		} else if (config.strict().equals(Strict.NONE)) {
			//4. 只匹配表和库, 此时表和库都没匹配上, 则不继续匹配
			//5. NONE最宽松的匹配, 匹配到default库的同名表
			hiveDatabase = "default.";
			hiveTable = mysqlTable;
			mode = HiveMode.APPEND;
		}
		return new HiveTable(hiveDatabase, hiveTable, mode, primaryKey);
	}

	private String makeHQL(HiveTable hiveTable, Map<String, Object> data, String type) {
		if (type.equals("insert")) {
			return makeInsertHQL(hiveTable.fullTable, data);
		} else if (type.equals("update")) {
			return makeUpdateHQL(hiveTable.fullTable, hiveTable.primaryKey, data);
		}
		return "";
	}

	private String makeInsertHQL(String hiveTableFullName, Map<String, Object> data) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(hiveTableFullName).append(" (");
		for (String column : data.keySet()) {
			sb.append(column).append(",");
		}
		sb.deleteCharAt(sb.length() - 1).append(") VALUES (");
		for (String column : data.keySet()) {
			sb.append("'").append(data.get(column)).append("',");
		}
		sb.deleteCharAt(sb.length() - 1).append(")");
		return sb.toString();
	}

	private String makeUpdateHQL(String hiveTableFullName, String primaryKey, Map<String, Object> data) {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ").append(hiveTableFullName).append(" SET ");
		for (String column : data.keySet()) {
			sb.append(column).append(" = '").append(data.get(column)).append("',");
		}
		sb.deleteCharAt(sb.length() - 1).append(" WHERE ").append(primaryKey).append(" = '").append(data.get(primaryKey)).append("'");
		return sb.toString();
	}

	@Override
	public void requestStop() {
		//关闭Hive连接
		connPool.close();
	}

	@Override
	public void awaitStop(Long aLong) {

	}
}
