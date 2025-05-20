package doris_tool_pool;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnectionPoolNew {

    private static DruidDataSource dataSource;

    static {
        // 创建 Druid 数据源实例
        dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:mysql://127.0.0.1:9030/demo");
        dataSource.setUsername("root");
        dataSource.setPassword("why123");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // 设置连接池相关参数
        dataSource.setTestOnBorrow(true);         // 设置借用连接时是否验证连接有效性
        dataSource.setMaxActive(900);             // 最大连接数
        dataSource.setMinIdle(5);                 // 最小空闲连接数
        dataSource.setInitialSize(5);             // 初始连接池大小
        dataSource.setMaxWait(30000);             // 获取连接的最大等待时间（毫秒）
        dataSource.setTimeBetweenEvictionRunsMillis(30000); // 设置检测空闲连接的时间间隔
        dataSource.setMinEvictableIdleTimeMillis(60000);     // 设置空闲连接最小生存时间
        dataSource.setRemoveAbandoned(true);      // 是否启用连接泄漏检查
        dataSource.setRemoveAbandonedTimeout(180); // 设置连接泄漏的超时时间（单位：秒）
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void close() {
        dataSource.close();
    }
}
