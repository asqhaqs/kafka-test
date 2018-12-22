package cn.situation.util;

import cn.situation.cons.SystemConstant;
import org.slf4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lenzhao
 * @date 2018/12/4 10:40
 * @description TODO
 */
public class SqliteUtil {

    private static final Logger LOG = LogUtil.getInstance(SqliteUtil.class);

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;

    private static SqliteUtil instance = new SqliteUtil();

    public static SqliteUtil getInstance() {
        return instance;
    }

    /**
     * 获取数据库连接
     * @param dbFilePath
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public Connection getConnection(String dbFilePath) throws ClassNotFoundException, SQLException {
        LOG.info(String.format("[%s]: dbFilePath<%s>", "getConnection", dbFilePath));
        Connection conn;
        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbFilePath);
        return conn;
    }

    public String getQuerySql() {
        return "SELECT * FROM t_position;";
    }

    public String getUpdateSql(String kind, String type, String fileName) {
        int position = FileUtil.getPositionByFileName(fileName);
        return "UPDATE t_position SET file_name='" + fileName + "',position=" + position + " WHERE kind='" + kind
                + "' AND type='" + type + "'";
    }

    /**
     * 执行sql查询
     * @param sql sql select 语句
     * @return 查询结果
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public synchronized Map<String, Integer> executeQuery(String sql) {
        Map<String, Integer> map = new HashMap<>();
        try {
            resultSet = getStatement().executeQuery(sql);
            while ( resultSet.next() ) {
                String type = resultSet.getString("type");
                int position = resultSet.getInt("position");
                map.put(type, position);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            destroyed();
        }
        return map;
    }

    /**
     * 执行数据库更新sql语句
     * @param sql
     * @return 更新行数
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public synchronized void executeUpdate(String sql) {
        try {
            getStatement().executeUpdate(sql);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            destroyed();
        }

    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        if (null == connection) {
            connection = getConnection(SystemConstant.SQLITE_DB_PATH);
        }
        return connection;
    }

    private Statement getStatement() throws SQLException, ClassNotFoundException {
        if (null == statement) {
            statement = getConnection().createStatement();
        }
        return statement;
    }

    /**
     * 资源释放
     */
    public void destroyed() {
        try {
            if (null != resultSet) {
                resultSet.close();
                resultSet = null;
            }
            if (null != statement) {
                statement.close();
                statement = null;
            }
            if (null != connection) {
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

}
