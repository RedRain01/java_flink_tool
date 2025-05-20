package doris_tool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnectionPoolNew {

    private static HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:9030/demo");
        config.setUsername("root");
        config.setPassword("why123");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // 设置连接池相关参数
        config.setConnectionTestQuery("SELECT 1");
        config.setMaximumPoolSize(500); // 最大连接数
        config.setMinimumIdle(5);       // 最小空闲连接数
        config.setIdleTimeout(30000);   // 空闲连接超时
        config.setConnectionTimeout(30000); // 获取连接的最大等待时间
        config.setMaxLifetime(600000);   // 连接的最大生命周期

        dataSource = new HikariDataSource(config);
        dataSource.setMaximumPoolSize(900);
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void close() {
        dataSource.close();
    }
}
