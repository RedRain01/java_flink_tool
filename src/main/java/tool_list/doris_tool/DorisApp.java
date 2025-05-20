package tool_list.doris_tool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DorisApp {

    public static void main(String[] args) {
        // 假设有多个查询任务
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DatabaseConnectionPoolNew.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT * from ticket_date");
            while (resultSet.next()) {
                DorisQueryExecutor.executeQuery(resultSet.getString("order_code"),resultSet.getString("order_date"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // 等待所有任务完成
         DorisQueryExecutor.shutdown();
        // 关闭连接池
         DatabaseConnectionPoolNew.close();
    }
}

