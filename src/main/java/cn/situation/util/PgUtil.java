package cn.situation.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.situation.cons.SystemConstant;

/**
 * postgresql数据库连接工具类
 * @author quanli
 */
public class PgUtil {
private final Logger logger = LoggerFactory.getLogger(PgUtil.class);
	
	private static PgUtil instance = new PgUtil();
	private Connection conn = null;
	private PreparedStatement pre = null;
	private ResultSet res = null;
	private static BasicDataSource dbSource;
	
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
		dbSource.setDefaultAutoCommit(true);
		dbSource.setMaxWaitMillis(maxSeconds * 1000);
	}
	
	public static PgUtil getInstance() {
		return instance;
	}
	
	/**
	 * 初始化pg数据库连接
	 * @return
	 */
	public Connection getConnection() {
		if(conn != null) {
			return conn;
		}
		try {
			conn = dbSource.getConnection();
			System.out.println("111111111111111111");
			logger.debug(String.format("[%s]: conn<%s>", "getConnection", conn));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return conn;
	}
	
	public PreparedStatement getPreparedStatement(String sql) throws Exception {
		return getConnection().prepareStatement(sql);
	}
	
	/**
	 * 销毁连接信息
	 */
	public void destory() {
		try {
			if(res != null) {
				res.close();
				res = null;
			}
			if(pre != null) {
				pre.close();
				pre = null;
			}
			if(conn != null) {
				conn.close();
				conn = null;
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
}
