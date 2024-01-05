package com.amarsoft.rwa.engine.me.util;

import cn.hutool.extra.spring.SpringUtil;
import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 获取数据库连接
 */
@Slf4j
public class DBUtils {
    /**
     * 最大连接次数
     */
    public static final int MAX_INDEX = 10;

    /**
     * 获取数据库连接
     * @return
     */
    public static Connection getConnection() throws SQLException {
        Connection conn = null;
        int index = 1;
        while (index < MAX_INDEX) {
            try {
                if (conn != null && !conn.isClosed()) {
                    return conn;
                }
                conn = SpringUtil.getBean(JdbcTemplate.class).getDataSource().getConnection();
            } catch (SQLException e) {
                log.error("第" + index + "次获取连接失败，失败原因：", e);
            }
            index++;
        }
        throw new SQLException("获取连接异常");
    }
    /**
     * 清空数据分组临时表数据
     * @param db 数据库处理对象
     * @return 返回操作影响记录数
     * @throws EngineSQLException 数据库操作异常
     */
    public static int clearTableTemp(DBConnection db, String tableName) throws EngineSQLException {
        String sql = "";
        if ("oracle".equals(EngineJBO.dbType)) {
            sql = "TRUNCATE TABLE " + tableName;
        } else if ("db2".equals(EngineJBO.dbType)) {
            sql = "alter table " + tableName + " activate not logged initially with empty table";
        }
        int count = db.executeUpdate(sql);
        log.info("清除表{}数据完成",tableName);
        return count;
    }
    public static int delTableResult(DBConnection db,String tableName,String resultNo) {
        int count  = 0;
        try {
            String sql = "DELETE FROM "+tableName+" where ResultSerialNo ='"+ resultNo +"'";
            count = db.executeUpdate(sql);
            log.info("清除表{}数据完成,结果流水号为：{}",tableName, resultNo);
        }catch (EngineSQLException e) {
            log.info("清除表" + tableName + "数据失败,结果流水号为：" + resultNo + "，失败原因：", e);
        }
        return count;
    }
    /**
     * 表统计分析
     */
    public static void analyzeTable(String tableName) {
 /*       // 数据库连接
        DBConnection db = new DBConnection();
        try {
            // 创建连接
            db.createConnection();
            String sql = "";
            sql = "analyze " + tableName + " ";
            db.executeUpdate(sql);
            log.info("gs表" + tableName + "分析成功");
        } catch (Exception e) {
            log.error("gs表分析失败,失败原因：", e);
        } finally {
            db.close();
        }*/
    }
}
