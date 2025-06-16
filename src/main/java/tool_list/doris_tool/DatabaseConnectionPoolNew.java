package tool_list.doris_tool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnectionPoolNew {

    private static HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://192.168.1.5:9030/demo?query_timeout=300&rewriteBatchedStatements=true&useSSL=false&serverTimezone=UTC");
        config.setUsername("root");
        config.setPassword("");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        config.setLeakDetectionThreshold(10_000); // 超过10秒未关闭连接就警告
        config.setMaximumPoolSize(30);            // 最大并发连接数
        config.setMinimumIdle(5);
        config.setIdleTimeout(600_000);           // 10分钟
        config.setConnectionTimeout(30_000);      // 获取连接最多等30秒
        config.setMaxLifetime(1_800_000);         // 最长存活30分钟
        config.setKeepaliveTime(60_000);          // 每分钟 ping Doris 防止断连
        config.setValidationTimeout(3_000);       // 测试连接有效性超时时间
        config.setConnectionTestQuery("SELECT 1");
        config.setAutoCommit(false);
        // 空闲连接每  秒 ping 一次数据库（防止被 kill）

        dataSource = new HikariDataSource(config);
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void close() {
        dataSource.close();
    }
}
