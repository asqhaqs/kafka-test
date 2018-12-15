package cn.situation.util;

import java.sql.Connection;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import cn.situation.cons.SystemConstant;

/**
 * postgresql数据库连接工具类
 * @author quanli
 */
public class PgUtil {
	private static final Logger LOG = LogUtil.getInstance(PgUtil.class);
	
	private Connection conn = null;
	private static volatile BasicDataSource dbSource;
	
	static {
		String url = SystemConstant.POSTGRESQL_URL;
		String userName = SystemConstant.POSTGRESQL_USERNAME;
		String password = SystemConstant.POSTGRESQL_PASSWORD;
		int minidle = Integer.parseInt(SystemConstant.DB_MINIDLE);
		int maxidle = Integer.parseInt(SystemConstant.DB_MAXIDLE);
		int maxSeconds = Integer.parseInt(SystemConstant.DB_MAXWAIT_SECONDS);
		int maxTotal = Integer.parseInt(SystemConstant.DB_MAXWAIT_SECONDS);
		
		dbSource = new BasicDataSource();
		dbSource.setDriverClassName("org.postgresql.Driver");
		dbSource.setUsername(userName);
		dbSource.setPassword(password);
		dbSource.setUrl(url);
       
		dbSource.setMinIdle(minidle);
		dbSource.setMaxIdle(maxidle);
		dbSource.setMaxTotal(maxTotal);
		dbSource.setRemoveAbandonedOnBorrow(true);
		dbSource.setRemoveAbandonedOnMaintenance(true);
		dbSource.setRemoveAbandonedTimeout(60);
		dbSource.setDefaultAutoCommit(true);
		dbSource.setMaxWaitMillis(maxSeconds * 1000);
	}
	
	/**
	 * 初始化pg数据库连接
	 * @return
	 */
	public Connection getConnection() {
		try {
			conn = dbSource.getConnection();
			LOG.debug(String.format("[%s]: conn<%s>", "getConnection", conn));
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return conn;
	}
}
