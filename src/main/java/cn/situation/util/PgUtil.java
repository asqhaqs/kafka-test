package cn.situation.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
			String url = SystemConstant.POSTGRESQL_URL;
			String userName = SystemConstant.POSTGRESQL_USERNAME;
			String password = SystemConstant.POSTGRESQL_PASSWORD;
			
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(url, userName, password);
			conn.setAutoCommit(true);
		} catch (Exception e) {
			logger.error(String.format("postgresql连接信息获取失败, 错误信息[%s]", e.getMessage()));
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
			logger.error(String.format("连接信息关闭失败, 错误信息为:[%s]", e.getMessage()));
		}
	}
	
}
