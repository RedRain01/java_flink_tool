package doris_tool_pool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Queue;

public class DorisApp {

    public static void main(String[] args) {
        // 假设有多个查询任务
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 获取当前JVM的Runtime实例
            Runtime runtime = Runtime.getRuntime();

            // 打印JVM总内存、已分配内存和剩余内存
            long totalMemory = runtime.totalMemory(); // JVM的总内存
            long freeMemory = runtime.freeMemory(); // 当前空闲内存
            long maxMemory = runtime.maxMemory(); // JVM的最大内存
            long usedMemory = totalMemory - freeMemory; // 已使用内存

            System.out.println("Total memory (bytes): " + totalMemory);
            System.out.println("Free memory (bytes): " + freeMemory);
            System.out.println("Max memory (bytes): " + maxMemory);
            System.out.println("Used memory (bytes): " + usedMemory);
            Queue<String[]> dataQueue = new LinkedList<>();



            connection = DatabaseConnectionPoolNew.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT * from ticket_date");
            int size = resultSet.getRow();
            int size1 = 0;
            System.out.println("--666666666666666666-------------------------"+size);
            while (resultSet.next()) {
                dataQueue.add(new String[]{resultSet.getString("order_code"), resultSet.getString("order_date")});
              //  DorisQueryExecutor.executeQuery();
            }
            System.out.println("--222222222222222222222222-------------------------"+size1);

        } catch (SQLException e) {
            System.out.println("--111222333-------------------------"+e.getMessage());
        }
        // 等待所有任务完成

        // 关闭连接池
        System.out.println("--33333333333333333333333333-------------------------");

        DatabaseConnectionPoolNew.close();
    }
}

