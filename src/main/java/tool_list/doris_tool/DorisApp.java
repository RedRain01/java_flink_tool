package tool_list.doris_tool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class DorisApp {


    public static void main(String[] args) {
        // 生产者和消费者两个线程都要完成
        CountDownLatch latch = new CountDownLatch(2);

        // 标记写入完成，供消费者线程判断队列是否已无新数据
        AtomicBoolean writingCompleted = new AtomicBoolean(false);

        // 启动消费者线程（监听队列，批量写入Doris）
        Thread monitorThread = new Thread(() -> {
            try {
                System.out.println("新增doris数---------------");
                //   DorisQueryExecutor.writeAllQueueToCsv(writingCompleted,"/mnt/data3/ticket_date/test_csv/result.csv");
                         DorisQueryExecutor.monitorAndInsertToDoris(writingCompleted);
            } catch (Exception e) {
                System.out.println("新增doris数据库失败");
                e.printStackTrace();
            } finally {
                latch.countDown(); // 消费线程结束
            }
        });
        monitorThread.start();

        // 生产者线程（查询数据并写入队列）
        Thread producerThread = new Thread(() -> {
            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;

            List<Future<?>> futures = new ArrayList<>();

            try {
                System.out.println("开始清洗数据---------------");
                connection = DatabaseConnectionPoolNew.getConnection();
                statement = connection.createStatement();

                //    resultSet = statement.executeQuery("SELECT * from ticket_date_new WHERE ts_code='000159.SZ'  AND trade_date='20241118'");
                 resultSet = statement.executeQuery("SELECT * from ticket_date_new");
                while (resultSet.next()) {
                    Future<?> future = DorisQueryExecutor.executeQuery(
                            resultSet.getString("ts_code"),
                            resultSet.getString("trade_date"),
                            resultSet.getBigDecimal("up_limit"),
                            resultSet.getBigDecimal("down_limit")
                    );
                    futures.add(future);
                }
                for (Future<?> future : futures) {
                    future.get();  // 会阻塞直到该任务完成
                }
            } catch (Exception e) {
                System.out.println("-----------SQLException------------------" + e.getMessage());
                e.printStackTrace();
            } finally {
                writingCompleted.set(true);  // 标记写完了，不会再往队列放数据
                latch.countDown();           // 生产线程结束
                // 释放资源
                try {
                    if (resultSet != null) resultSet.close();
                    if (statement != null) statement.close();
                    if (connection != null) connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        producerThread.start();

        // 主线程等待两个线程都完成
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("主线程等待被中断");
        }

        // 两个线程完成后关闭资源和线程池
        DorisQueryExecutor.shutdown();

        System.out.println("所有任务完成，程序退出");
    }

}

