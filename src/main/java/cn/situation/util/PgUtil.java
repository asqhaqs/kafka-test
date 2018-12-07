package cn.situation.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;

import cn.situation.cons.SystemConstant;

/**
 * postgresql数据库连接工具类
 * @author quanli
 */
public class PgUtil {
	private final Logger logger = LogUtil.getInstance(PgUtil.class);
	
	private static PgUtil instance = new PgUtil();
	private Connection conn = null;
	
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
		} catch (Exception e) {
			logger.error(String.format("postgresql连接信息获取失败, 错误信息[%s]", e.getMessage()));
		}
		return conn;
	}
	
	/**
	 * 销毁连接信息
	 */
	public void destory(Connection connection, Statement stat, ResultSet res) {
		try {
			if(res != null) {
				res.close();
				res = null;
			}
			if(stat != null) {
				stat.close();
				stat = null;
			}
			if(connection != null) {
				connection.close();
				connection = null;
			}
		} catch (Exception e) {
			logger.error(String.format("连接信息关闭失败, 错误信息为:[%s]", e.getMessage()));
		}
	}
}
