package io.gitlab.leibnizhu.maxwell.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KerberosConfig {
	public String krb5ConfPath;
	public String kerberosUser;
	public String kerberosKeytabPath;

	public KerberosConfig(Properties prop) throws IllegalArgumentException {
		String krb5ConfPath = prop.getProperty("kerberos.krb5conf");
		if (krb5ConfPath == null) {
			throw new IllegalArgumentException("解析Kerberos配置时:配置要求开启Kerberos支持, 但krb5.conf路径(kerberos.krb5conf)尚未配置!");
		} else {
			this.krb5ConfPath = krb5ConfPath;
		}
		String kerberosUser = prop.getProperty("kerberos.user");
		if (kerberosUser == null) {
			throw new IllegalArgumentException("解析Kerberos配置时:配置要求开启Kerberos支持, 但Kerberos用户(kerberos.user)尚未配置!");
		} else {
			this.kerberosUser = kerberosUser;
		}
		String kerberosKeytabPath = prop.getProperty("kerberos.keytab");
		if (kerberosKeytabPath == null) {
			throw new IllegalArgumentException("解析Kerberos配置时:配置要求开启Kerberos支持, 但Kerberos的keytab路径(kerberos.keytab)尚未配置!");
		} else {
			this.kerberosKeytabPath = kerberosKeytabPath;
		}
	}
}
