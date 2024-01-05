package com.amarsoft.rwa.engine.me.util;

import cn.hutool.extra.spring.SpringUtil;
import com.amarsoft.rwa.engine.me.config.EngineConfig;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * 公共方法工具类，提供一些非业务的通用方法。
 *
 * @author 陈庆
 * @version 1.0 2013-06-05
 */
public class EngineUtil {

    /**
     * 默认完整日期格式
     */
    public static final String DATEDEFAULTPATTERN = "yyyy-MM-dd HH:mm:ss,SSS";
    /**
     * 数值格式化对象
     */
    public static NumberFormat numberFormat = null;

    /**
     * 根据给定模式格式化日期
     *
     * @param pattern 格式
     * @param d       日期
     * @return 返回给定格式的日期
     */
    public static String getFormatDate(String pattern, Date d) {
        String s = new SimpleDateFormat(pattern).format(d);
        return s;
    }

    /**
     * 根据给定模式格式化当前日期
     *
     * @param pattern 格式
     * @return 返回给定格式的当前日期
     */
    public static String getFormatDate(String pattern) {
        return getFormatDate(pattern, new Date());
    }

    /**
     * 根据默认模式格式化日期
     *
     * @param d 日期
     * @return 返回默认格式的日期
     */
    public static String getFormatDate(Date d) {
        return getFormatDate(DATEDEFAULTPATTERN, d);
    }

    /**
     * 返回默认格式的当前日期
     *
     * @return 返回默认格式的当前日期
     */
    public static String getFormatDate() {
        return getFormatDate(new Date());
    }

    /**
     * 根据给定日期格式将字符串转成日期
     *
     * @param pattern 日期格式
     * @param d       日期字符串
     * @return 根据给定日期格式将字符串转成日期
     * @throws ParseException 若转换失败则抛出异常
     */
    public static Date getFormatDate(String pattern, String d) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        dateFormat.setLenient(false);
        return dateFormat.parse(d);
    }

    /**
     * 检查传入的日期字符串格式，若为指定格式返回true，否则返回false
     *
     * @param d 日期字符串
     * @return 若为指定格式日期字符串返回true，否则false
     */
    public static boolean checkDateParam(String d) {
        try {
            Date date = getFormatDate("yyyyMMdd", d);
            if (date == null) {
                return false;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 返回数据库日期
     *
     * @param d 日期
     * @return 返回数据库日期
     */
    public static java.sql.Date getFormatSqlDate(Date d) {
        return new java.sql.Date(d.getTime());
    }

    /**
     * 返回数据库时间
     *
     * @param d 日期
     * @return 返回数据库时间
     */
    public static java.sql.Timestamp getFormatSqlTimestamp(Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    /**
     * 将日期字符串转为数据库日期(默认格式yyyyMMdd)
     *
     * @param d 日期字符串
     * @return 将日期字符串转为数据库日期
     * @throws ParseException 若转换失败则抛出异常
     */
    public static java.sql.Date getFormatSqlDate(String d) throws ParseException {
        return getFormatSqlDate(getFormatDate("yyyy-MM-dd", d));
    }

    /**
     * 返回数值格式化后的字符串(默认最大小数位6位且无三位一逗)
     *
     * @param d 数值
     * @return 返回数值格式化后的字符串
     */
    public static String format(double d) {
        return format(d, 6, false);
    }

    /**
     * 格式化后数值(无三位一逗)
     *
     * @param d      需格式化的数值
     * @param length 最大小数位
     * @return 格式化后数值
     */
    public static String format(double d, int length) {
        return format(d, length, false);
    }

    /**
     * 格式化后数值
     *
     * @param d            需格式化的数值
     * @param length       最大小数位
     * @param groupingUsed 是否三位一逗
     * @return 格式化后数值
     */
    public static String format(double d, int length, boolean groupingUsed) {
        if (numberFormat == null) {
            numberFormat = NumberFormat.getNumberInstance();
        }
        numberFormat.setGroupingUsed(groupingUsed);
        numberFormat.setMaximumFractionDigits(length);
        return numberFormat.format(d);
    }

    /**
     * yyyyMMdd格式日期字符串转为yyyy-MM-dd
     *
     * @param d 日期字符串
     * @return 返回转换后的日期字符串
     */
    public static String parseDate(String d) {
        return d.substring(0, 4) + "-" + d.substring(4, 6) + "-" + d.substring(6);
    }

    /**
     * 返回给定Properties文件路劲的Properties类
     *
     * @param propName Properties文件路劲
     * @return 返回Properties类
     * @throws IOException 文件路劲错误或者该文件非Properties文件
     */
    public static Properties getProperties(String propName) throws IOException {
        InputStream is = null;
        try {
            File f = new File(propName);
            Properties prop = new Properties();
            is = new FileInputStream(f);
            prop.load(is);
            return prop;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /**
     * 根据给定格式长度格式化数字，返回格式化后的字符串
     * <br>若数字未到指定格式长度，则在字符串前补0，否则返回该数字的字符串
     *
     * @param n 需要格式化的数字
     * @param p 格式长度
     * @return 返回数字根据给定格式长度格式化后的字符串
     */
    public static String getFormatStringOfNum(long n, int p) {
        StringBuffer sb = new StringBuffer();
        String s = "" + n;
        int ns = s.length();
        if (ns < p) {
            for (int i = 0; i < p - ns; i++) {
                sb.append("0");
            }
        }
        sb.append(n);
        return sb.toString();
    }

    /**
     * 将时间(毫秒数)转为指定格式的字符串
     * <br>若<code>t</code>为0，则格式为00:00:00.000, 否则为00:00:00
     *
     * @param time 时间
     * @param t    格式
     * @return 返回指定格式的字符串
     */
    public static String changeTimeToString(long time, int t) {
        String s = "";
        String sss = "";
        if (t == 0) {
            sss = "." + getFormatStringOfNum(time % 1000, 3);
        }
        time = time / 1000;
        long hrs = time / 3600;
        long min = (time % 3600) / 60;
        long sec = time % 60;
        s = getFormatStringOfNum(hrs, 2) + ":" + getFormatStringOfNum(min, 2) + ":" + getFormatStringOfNum(sec, 2) + sss;
        return s;
    }

    /**
     * 将时间(毫秒数)转为默认格式的字符串，默认格式为00:00:00.000
     *
     * @param time 时间
     * @return 返回指定格式的字符串
     */
    public static String changeTimeToString(long time) {
        return changeTimeToString(time, 0);
    }

    /**
     * 根据给定字段代码、字段值返回SQL条件语句
     * <br>若字段值为空或空字符串，则无条件语句，否则根据相应字段代码、字段值生成 column='columnValue'的SQL条件语句
     *
     * @param columnName  字段代码
     * @param columnValue 字段值
     * @return 返回SQL的条件语句
     */
    public static String getSqlAnd(String columnName, String columnValue) {
        String s = "";
        if ((columnValue != null) && (!"".equals(columnValue.trim()))) {
            s = " AND " + columnName + "='" + columnValue + "' ";
        }
        return s;
    }

    /**
     * 根据给定字段代码、字段值返回SQL条件语句
     * <br>若字段值为空或空字符串，则无条件语句，否则根据相应字段代码、字段值生成 column LIKE 'columnValue%'的SQL条件语句
     *
     * @param columnName  字段代码
     * @param columnValue 字段值
     * @return 返回SQL的条件语句
     */
    public static String getSqlAndLeftLike(String columnName, String columnValue) {
        String s = "";
        if ((columnValue != null) && (!"".equals(columnValue.trim()))) {
            s = " AND " + columnName + " LIKE '" + columnValue + "%' ";
        }
        return s;
    }

    /**
     * 根据给定字段代码、字段值返回机构相关的left like SQL条件语句
     * <br>若字段值为空或空字符串，则无条件语句，否则根据相应字段代码、字段值生成 机构的left LIKE SQL条件语句
     * <br>机构特殊处理
     *
     * @param columnName  字段代码
     * @param columnValue 字段值
     * @return 返回SQL的条件语句
     */
    public static String getSqlAndOrgLeftLike(String columnName, String columnValue) {
        String s = "";
        //TODO 机构函数可能调整
        if ((columnValue != null) && (!"".equals(columnValue.trim()))) {
            s = " AND " + columnName + " LIKE GetOrgSortNo('" + columnValue + "') || '%' ";
        }
        return s;
    }

    /**
     * 返回异常描述
     *
     * @param t 异常类
     * @return 返回异常描述
     */
    public static String getExceptionInfo(Throwable t) {
        String s = t.toString();
        StackTraceElement[] sts = t.getStackTrace();
        for (StackTraceElement st : sts) {
            s = s + "\n\tat " + st.toString();
        }
        s = getExceptionInfoByCause(t.getCause(), s);
        return s;
    }

    /**
     * 返回导致异常的异常描述
     *
     * @param t 异常类
     * @param s 异常描述
     * @return 返回导致异常的异常描述
     */
    public static String getExceptionInfoByCause(Throwable t, String s) {
        if (t != null) {
            s = s + "\nCaused by: " + t.toString();
            for (StackTraceElement st : t.getStackTrace()) {
                s = s + "\n\tat " + st.toString();
            }
            s = getExceptionInfoByCause(t.getCause(), s);
        }
        return s;
    }

    /**
     * 返回工程HOME目录
     */
    public static String getHomePath() {
        return System.getenv().get("ENGINE_HOME");
    }

    /**
     * 获取格式化后的日志信息
     *
     * @param type 日志类型
     * @param msg  日志信息
     * @return 返回格式化后的日志信息
     */
    private static String getLogString(String type, String msg) {
        return getFormatDate() + " [" + type + "] - " + msg;
    }

    /**
     * 获取基本日志信息
     *
     * @param msg 日志信息
     * @return 返回基本日志信息
     */
    public static String info(String msg) {
        return getLogString("INFO", msg);
    }

    /**
     * 获取详细日志信息
     *
     * @param msg 日志信息
     * @return 返回详细日志信息
     */
    public static String debug(String msg) {
        return getLogString("DEBUG", msg);
    }

    /**
     * 获取警告日志信息
     *
     * @param msg 日志信息
     * @return 返回警告日志信息
     */
    public static String warn(String msg) {
        return getLogString("WARN", msg);
    }

    /**
     * 获取错误日志信息
     *
     * @param msg 日志信息
     * @return 返回错误日志信息
     */
    public static String error(String msg) {
        return getLogString("ERROR", msg);
    }


    /**
     * 初始化引擎相关参数
     *
     * @throws Exception 若有部分参数未设置，则抛出异常
     */
    public static void initEngine() throws Exception {
        EngineConfig engineConfig = SpringUtil.getBean(EngineConfig.class);
        EngineJBO.batchCount = engineConfig.getBatchCount();
        EngineJBO.checkFalseResult = engineConfig.getCheckFalseResult();
        EngineJBO.checkRunFlag = engineConfig.getCheckRunFlag();
        EngineJBO.orgCode = engineConfig.getOrgCode();
        EngineJBO.subOrgList = EngineUtil.getListBySplit(engineConfig.getSubCompanyOrgID());
        EngineJBO.enableClientInfo = engineConfig.getEnableClientInfo();
    }

    /**
     * 将字符串根据空格 或则 逗号(,) 分割，返回分割后的非空字符串列表
     *
     * @param str 需分割的字符串
     * @return 返回分割后的非空字符串列表
     */
    private static List<String> getListBySplit(String str) {
        List<String> list = new ArrayList<String>();
        if (str != null) {
            for (String s1 : str.split(" ")) {
                for (String s : s1.split(",")) {
                    if (s != null && !"".equals(s)) {
                        list.add(s);
                    }
                }
            }
        }
        return list;
    }

    /**
     * 初始化计算任务计算相关信息类的数据
     */
    public static void initTaskJbo() {
        /** 结果流水号 */
        TaskJBO.resultNo = null;
        /** 任务类型 */
        TaskJBO.taskType = null;
        /** 数据日期 */
        TaskJBO.dataDate = null;
        /** 数据流水号 */
        TaskJBO.dataNo = null;
        /** 任务日志ID */
        TaskJBO.logID = null;
        /** 任务开始时间 */
        TaskJBO.startTime = null;
        /** 任务结束时间 */
        TaskJBO.overTime = null;
        /** 是否并表 默认非并表 市场风险暂时只支持非并表*/
        TaskJBO.consolidatedFlag = "0";
        /** 计算方案ID */
        TaskJBO.schemeID = null;
        /** 计算方案名称 */
        TaskJBO.schemeName = null;
        /** 市场风险计算方法 */
        TaskJBO.marketApproach = null;
        /** 利率一般风险计算方法 */
        TaskJBO.mirrgrApproach = null;
        /** 市场风险参数版本流水号 */
        TaskJBO.marketParamVerNo = null;
        /** 信用风险计算状态 */
        TaskJBO.creditState = null;
        /** 市场风险数据是否确认 */
        TaskJBO.confirmFlag = null;
        /** 当前计算步骤 */
        TaskJBO.currentStep = null;
        /** 当前计算步骤开始时间 */
        TaskJBO.currentStepTime = null;
        /** 风险加权资产 */
        TaskJBO.RWA = 0.0D;
        /** 资本要求 */
        TaskJBO.RC = 0.0D;
        /** 一般风险资本要求 */
        TaskJBO.GRCR = 0.0D;
        /** 特定风险资本要求 */
        TaskJBO.SRCR = 0.0D;
        /** 资产证券化特定风险资本要求 */
        TaskJBO.ABSSRCR = 0.0D;
        /** 利率特定风险资本要求 */
        TaskJBO.IRRSRCR = 0.0D;
        /** 利率一般风险资本要求 */
        TaskJBO.IRRGRCR = 0.0D;
        /** 股票特定风险资本要求 */
        TaskJBO.ERSRCR = 0.0D;
        /** 股票一般风险资本要求 */
        TaskJBO.ERGRCR = 0.0D;
        /** 外汇一般风险资本要求 */
        TaskJBO.FERGRCR = 0.0D;
        /** 商品一般风险资本要求 */
        TaskJBO.CRGRCR = 0.0D;
        /** 期权一般风险资本要求 */
        TaskJBO.ORGRCR = 0.0D;
        /** 期权得尔塔+法利率期权资本要求 */
        TaskJBO.ODIROCR = 0.0D;
        /** 期权得尔塔+法股票期权资本要求 */
        TaskJBO.ODEOCR = 0.0D;
        /** 期权得尔塔+法外汇期权资本要求 */
        TaskJBO.ODFEOCR = 0.0D;
        /** 期权得尔塔+法商品期权资本要求 */
        TaskJBO.ODCOCR = 0.0D;

        /** 利率风险资本要求*/
        TaskJBO.IRR_CR = 0.0D;
        /** 外汇风险资本要求*/
        TaskJBO.FER_CR = 0.0D;
        /** 商品风险资本要求*/
        TaskJBO.CR_CR = 0.0D;
        /** 股票风险资本要求*/
        TaskJBO.ER_CR = 0.0D;
    }

    public static void initConstantJbo(){
        /** 利率风险到期日法资本要求计提比率 */
        ConstantJBO.IRRMMCRPR = 0.0D;
        /** 利率风险久期法资本要求计提比率 */
        ConstantJBO.IRRDMCRPR = 0.0D;
        /** 股票风险特定市场风险计提比率 */
        ConstantJBO.ERSRCRPR = 0.0D;
        /** 股票风险一般市场风险计提比率 */
        ConstantJBO.ERGRCRPR = 0.0D;
        /** 外汇风险资本要求计提比率 */
        ConstantJBO.FERCRPR = 0.0D;
        /** 商品风险净头寸资本要求计提比率 */
        ConstantJBO.CRNPCRPR = 0.0D;
        /** 商品风险总头寸资本要求计提比率 */
        ConstantJBO.CRGPCRPR = 0.0D;
        /** 利率风险结果系数 */
        ConstantJBO.IRR_RF = 0.0D;
        /** 外汇风险结果系数 */
        ConstantJBO.FER_RF = 0.0D;
        /** 商品风险结果系数 */
        ConstantJBO.CR_RF = 0.0D;
        /** 股票风险结果系数 */
        ConstantJBO.ER_RF = 0.0D;
    }

}
