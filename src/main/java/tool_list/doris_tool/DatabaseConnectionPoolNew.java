package tool_list.doris_tool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnectionPoolNew {

    private static HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://192.168.1.5:9030/demo?autoReconnect=true&useSSL=false&serverTimezone=UTC");
        config.setUsername("root");
        config.setPassword("");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setLeakDetectionThreshold(10000); // 超过10秒未关闭连接，就记录日志

// 连接池核心配置
        config.setConnectionTestQuery("SELECT 1");
        config.setMaximumPoolSize(60);
        config.setMinimumIdle(5);
        config.setIdleTimeout(30000);         // 30 秒未使用则回收空闲连接
        config.setConnectionTimeout(300000);   // 300 秒获取不到连接就超时
        config.setMaxLifetime(1800000);        // 单个连接最多存活30 分钟
        config.setKeepaliveTime(15000);       // 空闲连接每 30 秒 ping 一次数据库（防止被 kill）

        dataSource = new HikariDataSource(config);
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void close() {
        dataSource.close();
    }
}
