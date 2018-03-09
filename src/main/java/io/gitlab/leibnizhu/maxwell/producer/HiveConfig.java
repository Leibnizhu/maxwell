package io.gitlab.leibnizhu.maxwell.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveConfig {
	private Logger log = LoggerFactory.getLogger(getClass());
	private Strict matchStrict; // 匹配的严格度
	private Map<String, HiveTable> tableMapping = new HashMap<>();
	private Map<String, HiveDatabase> databaseMapping = new HashMap<>();

	private static final Pattern TABLE_RULE = Pattern.compile("(\\w+?)\\.(\\w+?)->(\\w+?)\\.(\\w+?)(,[AM](,\\w+?)?)?");
	private static final Pattern DATABASE_RULE = Pattern.compile("(\\w+?)->(\\w+?)");

	HiveConfig(Properties prop) {
		parseStrict(prop);
		log.info("Hive映射规则严格程度为:{}", matchStrict);
		parseRule(prop);
		log.info("Hive表映射规则: {}", tableMapping);
		log.info("Hive数据库映射规则: {}", databaseMapping);
	}

	public boolean containDatabase(String database) {
		return databaseMapping.containsKey(database);
	}

	public boolean containTable(String database, String table) {
		return tableMapping.containsKey(database + "." + table);
	}

	public HiveDatabase hiveDatabase(String database) {
		return databaseMapping.get(database);
	}

	public HiveTable hiveTable(String database, String table) {
		return tableMapping.get(database + "." + table);
	}

	public Strict strict() {
		return matchStrict;
	}

	private void parseRule(Properties prop) {
		String[] rules = prop.getProperty("rule", "").split(";");
		for (String rule : rules) {
			Matcher tableMatcher = TABLE_RULE.matcher(rule);
			if (tableMatcher.matches()) {
				String mysqlDatabase = tableMatcher.group(1);
				String mysqlTable = tableMatcher.group(2);
				String hiveDatabase = tableMatcher.group(3);
				String hiveTable = tableMatcher.group(4);
				String modeStr = tableMatcher.group(5);
				HiveMode mode = (modeStr != null && modeStr.substring(1,2).equals("M")) ? HiveMode.MODIFY : HiveMode.APPEND;
				String primaryKey = Optional.ofNullable(tableMatcher.group(6)).orElse(",").substring(1);
				String mysqlFullTableName = mysqlDatabase + "." + mysqlTable;
				HiveTable tableRule = new HiveTable(hiveDatabase, hiveTable, mode, primaryKey);
				this.tableMapping.put(mysqlFullTableName, tableRule);
			} else {
				Matcher databaseMatcher = DATABASE_RULE.matcher(rule);
				if (databaseMatcher.matches()) {
					String mysqlDatabase = databaseMatcher.group(1);
					String hiveDatabase = databaseMatcher.group(2);
					HiveDatabase databaseRule = new HiveDatabase(hiveDatabase, HiveMode.APPEND);
					this.databaseMapping.put(mysqlDatabase, databaseRule);
				} else {
					if (rule.trim().length() == 0) {
						log.warn("映射规则为空, MySQL数据即将{}.", strict().equals(Strict.NONE) ? "保存到Hive的default库同名表中" : "不保存任何数据到Hive中");
					} else {
						log.warn("映射规则格式错误:'{}'", rule);
					}
				}
			}
		}
	}

	private void parseStrict(Properties prop) {
		String strictStr = prop.getProperty("strict", "table");
		try {
			this.matchStrict = Strict.valueOf(strictStr.toUpperCase());
		} catch (IllegalArgumentException e) {
			log.warn("Hive配置的strict参数为{}, 不在允许取值范围内(none/database/table), 使用默认值table", strictStr);
			this.matchStrict = Strict.TABLE;
		}
	}

	enum Strict {
		NONE, //无限制, 没有匹配则映射到Hive的default库的MySQL同名表
		DATABASE, //匹配表映射规则和数据库映射规则
		TABLE //只匹配表映射规则
	}

	enum HiveMode {
		APPEND, //只能插入(insert操作)
		MODIFY  //允许修改(update操作)
	}

	static class HiveTable {
		HiveTable(String database, String table, HiveMode mode, String keyColumn) {
			this.database = database;
			this.table = table;
			this.fullTable = database + "." + table;
			this.mode = mode;
			this.primaryKey = keyColumn;
		}

		String database;
		String table;
		String fullTable;
		String primaryKey;
		HiveMode mode;

		@Override
		public String toString() {
			return "HiveTable{" + "database='" + database + "', table='" + table + "', key='" + primaryKey + "', mode=" + mode + '}';
		}
	}

	static class HiveDatabase {
		HiveDatabase(String database, HiveMode mode) {
			this.database = database;
			this.mode = mode;
		}

		String database;
		HiveMode mode;

		@Override
		public String toString() {
			return "HiveDatabase{" + "database='" + database + "', mode=" + mode + '}';
		}
	}
}
