package com.amarsoft.rwa.engine.me.util.db;

import com.amarsoft.rwa.engine.me.exception.EngineConnectionException;
import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.util.DBUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Date;

/**
 * 获取数据库连接
 * @author IceBlue
 * @version 1.0 2013-06-05
 */
@Slf4j
public class DBConnection {

	/**
	 * 数据库连接对象
	 */
	private Connection conn = null;

	/**
	 * 更新数据库使用的Statement对象
	 */
	private Statement stmt = null;

	/**
	 * 获取数据库连接对象
	 * @return 数据库连接对象
	 */
	protected final Connection getConnection() {
		return this.conn;
	}

	/**
	 * 创建数据库连接
	 * @throws EngineConnectionException 数据库连接异常
	 */
	public final void createConnection() throws EngineConnectionException {
		try {
			conn = DBUtils.getConnection();
			stmt = conn.createStatement();
			// 确定数据库类型
			EngineJBO.dbType = "oracle";
		} catch (SQLException e) {
			e.printStackTrace();
			throw new EngineConnectionException("连接异常", e);
		} catch (Exception e) {
			e.printStackTrace();
			throw new EngineConnectionException("其他异常", e);
		}
	}

	/**
	 * 关闭数据库连接
	 */
	public final void close() {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				log.warn("关闭Statement异常", e);
			} finally {
				stmt = null;
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				log.warn("关闭数据库连接异常", e);
			} finally {
				conn = null;
			}
		}
	}

	/**
	 * 创建PreparedStatement
	 * @param sql 创建SQL语句
	 * @return PreparedStatement 创建PreparedStatement
	 * @throws EngineSQLException 创建PreparedStatement异常
	 */
	public final PreparedStatement prepareStatement(final String sql)
			throws EngineSQLException {
		PreparedStatement pst = null;
		try {
			pst = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new EngineSQLException("获取PreparedStatement异常[" + sql + "]", e);
		}
		return pst;
	}

	/**
	 * 创建Statement
	 * @return Statement
	 * @throws SQLException 创建Statement异常
	 */
	public final Statement createStatement() throws EngineSQLException {
		Statement st = null;
		try {
			st = conn.createStatement();
		} catch (SQLException e) {
			throw new EngineSQLException("获取Statement异常", e);
		}
		return st;
	}
	
	/**
	 * 创建CallableStatement
	 * @param sql SQL语句
	 * @return CallableStatement
	 * @throws EngineSQLException 获取CallableStatement异常
	 */
	public final CallableStatement prepareCall(String sql) throws EngineSQLException {
		CallableStatement cst = null;
		try {
			cst = conn.prepareCall(sql);
		} catch (SQLException e) {
			throw new EngineSQLException("获取CallableStatement异常[" + sql + "]", e);
		}
		return cst;
	}
	
	/**
	 * 获取ResultSet
	 * @param st 
	 * @param sql 
	 * @return ResultSet
	 * @throws EngineSQLException 执行查询sql异常
	 */
	public final ResultSet executeQuery(Statement st, String sql) throws EngineSQLException {
		ResultSet rs = null;
		try {
			Date start = new Date();
			rs = st.executeQuery(sql);
			Date end = new Date();
			log.debug("执行sql语句(" + (end.getTime() - start.getTime()) * 1.0 / 1000 + "):" + sql);
		} catch (SQLException e) {
			throw new EngineSQLException("获取ResultSet异常[" + sql + "]", e);
		}
		return rs;
	}
	
	/**
	 * 执行存储过程(1个字符串参数)，并返回执行结果
	 * @param sql 执行存储过程的SQL语句
	 * @param param 传入参数
	 * @return 返回执行结果
	 * @throws EngineSQLException 数据库操作异常
	 */
	public final String executeCallBy1String(String sql, String param) throws EngineSQLException {
		String msg = null;
		CallableStatement cst = null;
		try {
			cst = this.prepareCall(sql);
			cst.setString(1, param);
			cst.registerOutParameter(2, Types.VARCHAR);
			cst.registerOutParameter(3, Types.VARCHAR);
			cst.executeUpdate();
			// 获取执行返回信息
			msg = cst.getString(2);
			// 判断成功或失败，若执行失败，则取日志信息
			if ("0".equals(msg)) {
				msg = cst.getString(3);
			}
		} catch (EngineSQLException e) {
			throw e;
		} catch (SQLException e) {
			throw new EngineSQLException("执行CallableStatement异常[" + sql + "][" + param + "]", e);
		} finally {
			this.closeStatement(cst);
		}
		return msg;
	}
	
	/**
	 * 若无数据，返回null，有返回double结果
	 * @param rs 结果集
	 * @param name 字段名
	 * @return 若无数据，返回null，有返回double结果
	 * @throws SQLException 
	 */
	public final Double getDouble(ResultSet rs, String name) throws SQLException {
		if (rs.getString(name) == null) {
			return null;
		} else {
			return rs.getDouble(name);
		}
	}
	
	/**
	 * 若无数据，返回null，有返回int结果
	 * @param rs 结果集
	 * @param name 字段名
	 * @return 若无数据，返回null，有返回int结果
	 * @throws SQLException
	 */
	public final Integer getInt(ResultSet rs, String name) throws SQLException {
		if (rs.getString(name) == null) {
			return null;
		} else {
			return rs.getInt(name);
		}
	}
	
	/**
	 * 根据给定 数据 及 坐标 设置数据
	 * @param pst pst预处理对象
	 * @param index 坐标值
	 * @param value 数据
	 * @param type 数据类型
	 * @throws SQLException 若设定出错，抛出数据库操作异常
	 */
	public final void setPstValue(PreparedStatement pst, int index, Object value, int type) throws SQLException {
		// 判断 数据是否为空， 为空则设为空值
		// 否则判断数据类型，根据数据类型 设定相应的数据
		if (value == null) {
			pst.setNull(index, type);
		} else {
			switch (type) {
			case Types.INTEGER:
				pst.setInt(index, (Integer)value);
				break;
			case Types.DOUBLE:
				pst.setDouble(index, (Double)value);
				break;
			case Types.VARCHAR:
				pst.setString(index, (String)value);
				break;
			default:
				break;
			}
		}
	}

	/**
	 * 提交事务
	 * @throws EngineSQLException 数据库操作异常
	 */
	public final void commit() throws EngineSQLException {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new EngineSQLException("事务提交异常", e);
		}
	}

	/**
	 * 回滚事务
	 * @throws SQLException SQL异常
	 */
	public final void rollback() throws SQLException {
		conn.rollback();
	}

	/**
	 * 设置是否自动提交
	 * @throws SQLException SQL异常
	 */
	public final void setAutoCommit(boolean autoCommit) throws SQLException {
		conn.setAutoCommit(autoCommit);
	}

	/**
	 * 执行给定 SQL 语句，该语句可能为 INSERT、UPDATE 或 DELETE 语句，或者不返回任何内容的 SQL 语句（如 SQL
	 * DDL语句）,执行方式为同步方式
	 * @param sql SQL 语句
	 * @return 对于 SQL 数据操作语言 (DML) 语句，返回行计数 ，对于什么都不返回的 SQL 语句，返回 0
	 * @throws EngineSQLException 数据库操作异常类
	 */
	public final int executeUpdate(String sql) throws EngineSQLException {
		try {
			log.debug("执行sql语句[" + sql + "]");
			Date start = new Date();
			int result = stmt.executeUpdate(sql);
			Date end = new Date();
			log.debug("执行sql时间(" + (end.getTime() - start.getTime()) * 1.0 / 1000 + ")");
			return result;
		} catch (SQLException e) {
			throw new EngineSQLException("数据库执行异常\n", e);
		}
	}
	
	/**
	 * 执行给定 SQL 语句，该语句可能为 INSERT、UPDATE 或 DELETE 语句，或者不返回任何内容的 SQL 语句（如 SQL
	 * DDL语句）,执行方式为同步方式
	 * @param sql SQL 语句
	 * @return 对于 SQL 数据操作语言 (DML) 语句，返回行计数 ，对于什么都不返回的 SQL 语句，返回 0
	 * @throws EngineSQLException 数据库操作异常类
	 */
	public final int executeUpdate(StringBuffer sql) throws EngineSQLException {
		return this.executeUpdate(sql.toString());
	}
	
	/**
	 * PreparedStatement批量执行
	 * @param pst 
	 * @return 影响的记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	public final int executeBatch(PreparedStatement pst) throws EngineSQLException {
		try {
			int num = 0;
			num = pst.executeBatch().length;
			pst.clearBatch();
			return num;
		} catch (SQLException e) {
			throw new EngineSQLException("PreparedStatement批量执行异常", e);
		}
	}

	/**
	 * 关闭数据库ResultSet对象
	 * @param rs 数据库ResultSet对象
	 * @throws SQLException SQL异常
	 */
	public final void closeResultSet(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				log.warn("关闭ResultSet对象异常", e);
			}
		}
	}

	/**
	 * 关闭数据库Statement对象
	 * @param st 数据库Statement对象
	 */
	public final void closeStatement(Statement st) {
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				log.warn("关闭Statement对象异常", e);
			}
		}
	}

	/**
	 * 关闭数据库PreparedStatement对象
	 * @param pst 数据库PreparedStatement对象
	 */
	public final void closePrepareStatement(PreparedStatement pst) {
		if (pst != null) {
			try {
				pst.close();
			} catch (SQLException e) {
				log.warn("关闭PreparedStatement对象异常", e);
			}
		}
	}
}
