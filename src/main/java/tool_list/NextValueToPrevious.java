package tool_list;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

public class NextValueToPrevious {

    private static final String JDBC_URL = "jdbc:mysql://192.168.1.5:9030/demo";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "";

    private static final int BATCH_SIZE = 10000; // 每批读取1万行
    private static final int THREAD_POOL_SIZE = 8; // 8线程并行

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);

        // 先查所有 distinct code 进行分组并行
        List<String> codes = getDistinctCodes(conn);
        System.out.println("Total codes: " + codes.size());

        List<Future<?>> futures = new ArrayList<>();
        for (String code : codes) {
            futures.add(executor.submit(() -> {
                try {
                    processCode(code);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        // 等待所有任务完成
        for (Future<?> future : futures) {
            future.get();
        }
        executor.shutdown();
        conn.close();
        System.out.println("处理完成");
    }

    private static List<String> getDistinctCodes(Connection conn) throws SQLException {
        List<String> codes = new ArrayList<>();
        String sql = "SELECT DISTINCT ts_code FROM model_first_data";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                codes.add(rs.getString("code"));
            }
        }
        return codes;
    }

    private static void processCode(String code) throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        String query = "SELECT id, code, date, start, end FROM your_table WHERE code = ? ORDER BY date";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setString(1, code);
        ResultSet rs = pstmt.executeQuery();

        List<RowData> rows = new ArrayList<>();
        while (rs.next()) {
            rows.add(new RowData(
                    rs.getLong("id"),
                    rs.getString("code"),
                    rs.getDate("date"),
                    rs.getDouble("start"),
                    rs.getDouble("end")
            ));
        }
        rs.close();
        pstmt.close();

        // 计算滑动窗口：下一行 start = 上一行 end
        Double lastEnd = null;
        for (RowData row : rows) {
            if (lastEnd != null) {
                row.start = lastEnd;
            }
            lastEnd = row.end;
        }

        // 批量更新写回 Doris
        updateBatch(conn, rows);
        conn.close();
        System.out.println("处理完成 code: " + code);
    }

    private static void updateBatch(Connection conn, List<RowData> rows) throws SQLException {
        String updateSql = "UPDATE your_table SET start = ? WHERE id = ?";
        PreparedStatement pstmt = conn.prepareStatement(updateSql);
        int count = 0;
        for (RowData row : rows) {
            pstmt.setDouble(1, row.start);
            pstmt.setLong(2, row.id);
            pstmt.addBatch();

            if (++count % BATCH_SIZE == 0) {
                pstmt.executeBatch();
            }
        }
        pstmt.executeBatch();
        pstmt.close();
    }

    static class RowData {
        long id;
        String code;
        Date date;
        double start;
        double end;

        public RowData(long id, String code, Date date, double start, double end) {
            this.id = id;
            this.code = code;
            this.date = date;
            this.start = start;
            this.end = end;
        }
    }
}