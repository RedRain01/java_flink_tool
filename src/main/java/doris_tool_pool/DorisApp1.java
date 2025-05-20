package doris_tool_pool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.*;

public class DorisApp1 {

    // 设置核心线程数、最大线程数、队列大小等
    private static final int CORE_POOL_SIZE = 64;   // 核心线程数
    private static final int MAX_POOL_SIZE = 64;   // 最大线程数
    private static final int QUEUE_CAPACITY = 1500000;  // 队列容量
    private static final long KEEP_ALIVE_TIME = 60L; // 线程空闲最大存活时间（秒）

    public static void main(String[] args) {
        // 创建阻塞队列
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        // 创建线程池
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                CORE_POOL_SIZE,  // 核心线程数
                MAX_POOL_SIZE,   // 最大线程数
                KEEP_ALIVE_TIME, // 空闲线程存活时间
                TimeUnit.SECONDS, // 时间单位
                workQueue,       // 任务队列
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略
        );

        // 模拟生产者向队列中添加数据
        Thread producer = new Thread(() -> {
            try {
                // 假设有多个查询任务
                Connection connection = null;
                Statement statement = null;
                ResultSet resultSet = null;
                // 假设从数据库或者其他地方获取数据并放入队列
                connection = DatabaseConnectionPoolNew.getConnection();
                statement = connection.createStatement();
                                              //'2024-12-27','2024-12-30','2024-12-31','2025-01-02','2025-01-03','2025-01-06','2025-01-07','2025-01-08','2025-01-09'）
                 resultSet = statement.executeQuery("SELECT * from ticket_date11");
                //  resultSet = statement.executeQuery("SELECT * from ticket_date002 where order_code='600584' and order_date in ('2024-12-26','2024-12-27','2024-12-30','2024-12-31','2025-01-02','2025-01-03','2025-01-06','2025-01-07','2025-01-08','2025-01-09') ");
                while (resultSet.next()) {
                    executorService.submit(new ConsumerTask(new String[]{resultSet.getString("order_code"), resultSet.getString("order_date")})); // 提交消费者任务
                }
            } catch (Exception e) {
                System.out.println("--1Exception111111111111111111111-"+e.getMessage());
                Thread.currentThread().interrupt();
            }
        });

        // 启动生产者线程
        producer.start();

        // 等待生产者线程结束
        try {
            producer.join();
        } catch (InterruptedException e) {
            System.out.println("Producer-------111-------------- join interrupted");
            Thread.currentThread().interrupt();
        }

        // 关闭线程池
        executorService.shutdown();
    }

    // 消费者任务类
    static class ConsumerTask implements Runnable {
        private String[] record;

        public ConsumerTask(String[] record) {
            this.record = record;
        }

        @Override
        public void run() {
            // 消费任务：处理数据
           // System.out.println(Thread.currentThread().getName() + " code: " + record[0]+ " dete: " + record[1]);
            DorisQueryExecutor.executeQuery(record[0], record[1]);  // 处理数据
        }
    }
}
