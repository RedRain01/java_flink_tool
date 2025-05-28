package tool_list.doris_tool;

import org.checkerframework.checker.units.qual.N;
import tool_list.doris_tool.model.TicketDayPc001Count;
import tool_list.doris_tool.model.TicketPriceCount;
import tool_list.doris_tool.model.TicketStkLimit;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DorisQueryExecutor {
    private static final BlockingQueue<TicketDayPc001Count> queue = new LinkedBlockingQueue<>();
    private static final int BATCH_SIZE = 1000;

    // 创建一个固定大小的线程池来执行每个SQL任务
    private static final ExecutorService executorService = Executors.newFixedThreadPool(50);

    public static void executeQuery(String code,String date,BigDecimal upPrice,BigDecimal downPrice) {
        // 每个查询任务都交给线程池中的一个独立线程处理
        try{
        executorService.submit(() -> {
            Statement statement = null;
            BigDecimal startPrice=BigDecimal.ZERO;
            BigDecimal endPrice=null;
            String newCode = code.substring(0, code.length() - 3);
            String queryall="SELECT *  FROM  ticket_price_count tpc  WHERE  ts_code ='"+code+"' AND  trade_date ='"+date+"'";
            String queryStart="SELECT * FROM  ticket_detail  WHERE  order_code ='"+newCode+"' AND  order_date ='"+date+"' AND  tran_id IS NOT NULL order by order_time ASC limit  1";
            String queryEnd="SELECT * FROM ticket_detail  WHERE  order_code ='"+newCode+"' AND  order_date ='"+date+"' AND  tran_id IS NOT NULL order by order_time desc limit  1";
            try(Connection connection = DatabaseConnectionPoolNew.getConnection();
                PreparedStatement resultSet11 = connection.prepareStatement(queryall);
                PreparedStatement resultSetStart11 = connection.prepareStatement(queryStart);
                PreparedStatement resultSetEnd11 = connection.prepareStatement(queryEnd);
            )
            {
                List<TicketPriceCount> list=new ArrayList<>();
                statement = connection.createStatement();
                ResultSet resultSet=resultSet11.executeQuery();

//                ResultSetMetaData metaData = resultSet.getMetaData();
//                int columnCount = metaData.getColumnCount();
//                while (resultSet.next()){
//                    for (int i = 1; i <= columnCount; i++) {
//                        String columnName = metaData.getColumnName(i);
//                        Object value = resultSet.getObject(i);
//                        System.out.print(columnName + ": " + value + "\t");
//                    }
//                    System.out.println();
//                }
                while (resultSet.next()) {
                    TicketPriceCount ticketPriceCount=new TicketPriceCount();
                    ticketPriceCount.setOrderCode(resultSet.getString("ts_code"));
                    ticketPriceCount.setOrderDate(resultSet.getDate("trade_date"));
                    ticketPriceCount.setPrice(resultSet.getBigDecimal("price"));
                    ticketPriceCount.setVolumeSum(resultSet.getLong("volume_sum"));
                    ticketPriceCount.setSum(resultSet.getBigDecimal("sum"));
                    list.add(ticketPriceCount);
                    // 假设这里只是打印处理结果，你可以根据实际情况修改
                }
                ResultSet  resultSetStart = resultSetStart11.executeQuery();
                while (resultSetStart.next()){
                    startPrice = resultSetStart.getBigDecimal("price");
                }

                ResultSet  resultSetEnd = resultSetEnd11.executeQuery();
                // 处理查询结果
                while (resultSetEnd.next()){
                     endPrice = resultSetEnd.getBigDecimal("price");
                }
                // 使用 Stream API 按照 price 字段从小到大排序，得到新的 List
                List<TicketPriceCount> sortedList = list.stream()
                        .sorted(Comparator.comparing(TicketPriceCount::getPrice)) // 按照 price 从小到大排序
                        .collect(Collectors.toList()); // 收集成新的 List
                // 构造插入数据的 SQL 语句

                // 创建 PreparedStatement 对象

                // 使用 Stream 计算 sum 和 volume 的和，然后做除法
                // 使用 Stream 计算 sum 和 volume 的和，然后做除法
                BigDecimal evgPrice = BigDecimal.ZERO;
                BigDecimal evgUpPrice = BigDecimal.ZERO;
                BigDecimal evgDownPrice = BigDecimal.ZERO;
                AbstractMap.SimpleEntry<BigDecimal, Long> allReduce = sortedList.stream()
                        .map(ticket -> new AbstractMap.SimpleEntry<>(ticket.getSum(), ticket.getVolumeSum())) // 转换为 <BigDecimal, Long> 键值对
                        .reduce(
                                new AbstractMap.SimpleEntry<>(BigDecimal.ZERO, 0L), // 初始值
                                (acc, ticket) -> new AbstractMap.SimpleEntry<>(
                                        acc.getKey().add(ticket.getKey()), // sum 累加
                                        acc.getValue() + ticket.getValue()  // volume 累加
                                ),
                                (acc1, acc2) -> new AbstractMap.SimpleEntry<>(
                                        acc1.getKey().add(acc2.getKey()), // 合并 sum
                                        acc1.getValue() + acc2.getValue()  // 合并 volume
                                )
                        );// 计算 sum / volume

                //.map(acc -> acc.getValue() > 0 ? acc.getKey().divide(new BigDecimal(acc.getValue()), 2, BigDecimal.ROUND_HALF_UP) : BigDecimal.ZERO);
                // 设置参数
                BigDecimal startPrice1 = startPrice;
                List<TicketPriceCount> uplist = sortedList.stream().filter(ticketPriceCount -> ticketPriceCount.getPrice().compareTo(startPrice1) > 0).collect(Collectors.toList());
                List<TicketPriceCount> downlist = sortedList.stream().filter(ticketPriceCount -> ticketPriceCount.getPrice().compareTo(startPrice1) <0).collect(Collectors.toList());
                AbstractMap.SimpleEntry<BigDecimal, Long> upReduce = uplist.stream()
                        .map(ticket -> new AbstractMap.SimpleEntry<>(ticket.getSum(), ticket.getVolumeSum())) // 转换为 <BigDecimal, Long> 键值对
                        .reduce(
                                new AbstractMap.SimpleEntry<>(BigDecimal.ZERO, 0L), // 初始值
                                (acc, ticket) -> new AbstractMap.SimpleEntry<>(
                                        acc.getKey().add(ticket.getKey()), // sum 累加
                                        acc.getValue() + ticket.getValue()  // volume 累加
                                ),
                                (acc1, acc2) -> new AbstractMap.SimpleEntry<>(
                                        acc1.getKey().add(acc2.getKey()), // 合并 sum
                                        acc1.getValue() + acc2.getValue()  // 合并 volume
                                )
                        );// 计算 sum / volume

                AbstractMap.SimpleEntry<BigDecimal, Long> downReduce = downlist.stream()
                        .map(ticket -> new AbstractMap.SimpleEntry<>(ticket.getSum(), ticket.getVolumeSum())) // 转换为 <BigDecimal, Long> 键值对
                        .reduce(
                                new AbstractMap.SimpleEntry<>(BigDecimal.ZERO, 0L), // 初始值
                                (acc, ticket) -> new AbstractMap.SimpleEntry<>(
                                        acc.getKey().add(ticket.getKey()), // sum 累加
                                        acc.getValue() + ticket.getValue()  // volume 累加
                                ),
                                (acc1, acc2) -> new AbstractMap.SimpleEntry<>(
                                        acc1.getKey().add(acc2.getKey()), // 合并 sum
                                        acc1.getValue() + acc2.getValue()  // 合并 volume
                                )
                        );// 计算 sum / volume
                if (allReduce != null&&allReduce.getKey()!=null&&allReduce.getValue()!=null) {
                    if (BigDecimal.ZERO.compareTo(new BigDecimal(allReduce.getValue()))==0)  {
                        evgPrice=BigDecimal.ZERO;
                    }else {
                        evgPrice = allReduce.getKey().divide(new BigDecimal(allReduce.getValue()), 2, BigDecimal.ROUND_HALF_UP);
                    }
                }else {
                    evgPrice=BigDecimal.ZERO;
                }

                if (upReduce != null&&upReduce.getKey()!=null&&upReduce.getValue()!=null) {
                    if (BigDecimal.ZERO.compareTo(new BigDecimal(upReduce.getValue()))==0)  {
                        evgUpPrice=BigDecimal.ZERO;
                    }else {
                        evgUpPrice  = upReduce.getKey().divide(new BigDecimal(upReduce.getValue()), 2, BigDecimal.ROUND_HALF_UP);
                    }
                }else {
                    evgUpPrice=BigDecimal.ZERO;
                }

                if (downReduce != null&&downReduce.getKey()!=null&&downReduce.getValue()!=null) {
                    if (BigDecimal.ZERO.compareTo(new BigDecimal(downReduce.getValue()))==0)  {
                    }else {
                        evgDownPrice  = downReduce.getKey().divide(new BigDecimal(downReduce.getValue()), 2, BigDecimal.ROUND_HALF_UP);
                    }
                }else {
                    evgDownPrice=BigDecimal.ZERO;
                }

                BigDecimal stdPrice = weightedStdDev(sortedList);
                Optional<BigDecimal> minPrice = sortedList.stream().map(TicketPriceCount::getPrice)
                        .filter(Objects::nonNull)
                        .min(Comparator.naturalOrder());

                Optional<BigDecimal> maxPrice = sortedList.stream().map(TicketPriceCount::getPrice)
                        .filter(Objects::nonNull)
                        .max(Comparator.naturalOrder());

                Map<String, BigDecimal> bucketsMap = calculateVolumeBuckets(sortedList, upPrice, downPrice, BigDecimal.valueOf(allReduce.getValue()));

                TicketDayPc001Count ticketDayPc001Count= new TicketDayPc001Count();
                ticketDayPc001Count.setTicketCode(sortedList.get(0).getOrderCode());
                ticketDayPc001Count.setTradeDate((Date) sortedList.get(0).getOrderDate()); // 当前日期
                ticketDayPc001Count.setTicketDc(sortedList.get(0).getOrderCode() + "_" + sortedList.get(0).getOrderDate());

                ticketDayPc001Count.setPriceStart(startPrice);
                ticketDayPc001Count.setPriceEnd(endPrice);
                ticketDayPc001Count.setPriceMean(evgPrice);
                ticketDayPc001Count.setPriceMeanUp(evgUpPrice);
                ticketDayPc001Count.setPriceMeanDown(evgDownPrice);
                ticketDayPc001Count.setPriceOpen30Mean(BigDecimal.ZERO);
                ticketDayPc001Count.setPriceMedian(BigDecimal.ZERO);
                ticketDayPc001Count.setPriceStd(stdPrice);
                ticketDayPc001Count.setPriceMax(maxPrice.orElse(BigDecimal.ZERO));
                ticketDayPc001Count.setPriceMin(minPrice.orElse(BigDecimal.ZERO));
                ticketDayPc001Count.setPriceRange(
                        maxPrice.orElse(BigDecimal.ZERO).subtract(minPrice.orElse(BigDecimal.ZERO))
                );
                ticketDayPc001Count.setPriceSkewness(calculateSkewness(sortedList));
                ticketDayPc001Count.setPriceKurtosis(computeKurtosis(sortedList));

                ticketDayPc001Count.setVolumeTotal(allReduce.getValue());
                ticketDayPc001Count.setVolumeUp(upReduce.getValue());
                ticketDayPc001Count.setVolumeDown(downReduce.getValue());
                ticketDayPc001Count.setVolumeOpen30min(0L);

                // Up buckets（上涨量桶）
                ticketDayPc001Count.setVolumeU1Bucket(bucketsMap.getOrDefault("up1Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeU2Bucket(bucketsMap.getOrDefault("up2Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeU3Bucket(bucketsMap.getOrDefault("up3Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeU4Bucket(bucketsMap.getOrDefault("up4Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeU5Bucket(bucketsMap.getOrDefault("up5Bucket", BigDecimal.ZERO));

                // Down buckets（下跌量桶）
                ticketDayPc001Count.setVolumeD1Bucket(bucketsMap.getOrDefault("down1Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeD2Bucket(bucketsMap.getOrDefault("down2Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeD3Bucket(bucketsMap.getOrDefault("down3Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeD4Bucket(bucketsMap.getOrDefault("down4Bucket", BigDecimal.ZERO));
                ticketDayPc001Count.setVolumeD5Bucket(bucketsMap.getOrDefault("down5Bucket", BigDecimal.ZERO));




//                System.out.println("当前队列长度------：" + queue.size());
//                System.out.println("最大内存：" + Runtime.getRuntime().maxMemory() / 1024 / 1024 + "MB");
//                System.out.println("已用内存：" + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "MB");

                queue.put(ticketDayPc001Count);
            } catch (InterruptedException e) {
                Thread t = Thread.currentThread();
                System.err.println("⚠️ 当前线程被中断: " + t.getName());
                e.printStackTrace();
                Thread.currentThread().interrupt(); // 保留中断状态
            }//
             catch (Exception e) {
                System.out.println("----清洗数据异常------000---------"+e.getMessage());
                e.printStackTrace();
            }
        });
        }
        catch (Exception e) {
            System.out.println("----------222---------"+e.getMessage());
         }
    }


    public static  void monitorAndInsertToDoris(AtomicBoolean writingCompleted){
        try {
            List<TicketDayPc001Count> batch = new ArrayList<>();
            while (true) {
                TicketDayPc001Count item = queue.poll(1, TimeUnit.SECONDS);// 等待新数据
                if (item != null) {
                    batch.add(item);
                }
                if (batch.size()% 4000 == 0) { // 每 500 条提交一次
                    System.out.println("-----------提交-------"+batch.size());
                    insertDoris(batch);
                    continue;
                }
                if (writingCompleted.get()&&queue.isEmpty()&&batch.isEmpty()) {
                    System.out.println("-------sss----提交-------"+batch.size());
                    insertDoris(batch);
                    DatabaseConnectionPoolNew.close();
                    break;
                }
                System.out.println("-----99999------"+batch.size());
            }
        }catch (Exception e){
            System.out.println("-----------提交222-------"+e.getMessage());
        }

    }



    public static void writeAllQueueToCsv(AtomicBoolean writingCompleted, String csvFilePath) {
        List<TicketDayPc001Count> allData = new ArrayList<>();

        // 先不停地从队列里取数据，直到写入标志为true且队列为空
        while (!(writingCompleted.get() && queue.isEmpty())) {
            try {
                TicketDayPc001Count item = queue.poll(1, TimeUnit.SECONDS);
                if (item != null) {
                    allData.add(item);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("线程被中断，停止读取队列");
                break;
            }
        }

        System.out.println("从队列中读取到数据条数：" + allData.size());

        // 一次性写入CSV
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath))) {
            // 写表头
            writer.write("ticket_code,trade_date,ticket_dc,price_start,price_end,price_mean,price_mean_up,price_mean_down,price_open30_mean,price_median,price_std,price_max,price_min,price_range,price_skewness,price_kurtosis,volume_total,volume_up,volume_down,volume_open30min,volume_u1_bucket,volume_u2_bucket,volume_u3_bucket,volume_u4_bucket,volume_u5_bucket,volume_d1_bucket,volume_d2_bucket,volume_d3_bucket,volume_d4_bucket,volume_d5_bucket");
            writer.newLine();

            for (TicketDayPc001Count item : allData) {
                String line = String.format(
                        "%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
                        safeString(item.getTicketCode()),
                        item.getTradeDate(),
                        safeString(item.getTicketDc()),
                        safeDecimal(item.getPriceStart()),
                        safeDecimal(item.getPriceEnd()),
                        safeDecimal(item.getPriceMean()),
                        safeDecimal(item.getPriceMeanUp()),
                        safeDecimal(item.getPriceMeanDown()),
                        safeDecimal(item.getPriceOpen30Mean()),
                        safeDecimal(item.getPriceMedian()),
                        safeDecimal1(item.getPriceStd()),
                        safeDecimal(item.getPriceMax()),
                        safeDecimal(item.getPriceMin()),
                        safeDecimal(item.getPriceRange()),
                        safeDecimal1(item.getPriceSkewness()),
                        safeDecimal1(item.getPriceKurtosis()),
                        safeLong(item.getVolumeTotal()),
                        safeLong(item.getVolumeUp()),
                        safeLong(item.getVolumeDown()),
                        safeLong(item.getVolumeOpen30min()),
                        safeDecimal(item.getVolumeU1Bucket()),
                        safeDecimal(item.getVolumeU2Bucket()),
                        safeDecimal(item.getVolumeU3Bucket()),
                        safeDecimal(item.getVolumeU4Bucket()),
                        safeDecimal(item.getVolumeU5Bucket()),
                        safeDecimal(item.getVolumeD1Bucket()),
                        safeDecimal(item.getVolumeD2Bucket()),
                        safeDecimal(item.getVolumeD3Bucket()),
                        safeDecimal(item.getVolumeD4Bucket()),
                        safeDecimal(item.getVolumeD5Bucket())
                );
                writer.write(line);
                writer.newLine();
            }
            writer.flush();
            System.out.println("写入CSV完成");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 辅助函数
    private static String safeString(String s) {
        return s == null ? "" : s.replace(",", "");
    }
    private static Double safeDecimal(java.math.BigDecimal bd) {
        return bd == null ? 0 : bd.setScale(2, java.math.RoundingMode.HALF_UP).doubleValue();
    }

    private static Double safeDecimal1(java.math.BigDecimal bd) {
        return bd == null ? 0 : bd.setScale(8, java.math.RoundingMode.HALF_UP).doubleValue();
    }
    private static long safeLong(Long l) {
        return l == null ? 0L : l;
    }

    public static  void  insertDoris(List<TicketDayPc001Count> batch) {
       try {
           if (batch.size() >0) {
               String sql = "INSERT INTO ticket_day_pc00_count (" +
                       "ticket_code,"+
                       "trade_date,"+
                       "ticket_dc,"+
                       "price_start,"+
                       "price_end,"+
                       "price_mean,"+
                       "price_mean_up,"+
                       "price_mean_down,"+
                       "price_open30_mean,"+
                       "price_median,"+
                       "price_std,"+
                       "price_max,"+
                       "price_min,"+
                       "price_range,"+
                       "price_skewness,"+
                       "price_kurtosis,"+
                       "volume_total,"+
                       "volume_up,"+
                       "volume_down,"+
                       "volume_open30min,"+
                       "volume_u1_bucket,"+
                       "volume_u2_bucket,"+
                       "volume_u3_bucket,"+
                       "volume_u4_bucket,"+
                       "volume_u5_bucket,"+
                       "volume_d1_bucket,"+
                       "volume_d2_bucket,"+
                       "volume_d3_bucket,"+
                       "volume_d4_bucket,"+
                       "volume_d5_bucket) "+
                       "VALUES (?, " +
                       "?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?," +
                       " ?);";
               try(Connection connection = DatabaseConnectionPoolNew.getConnection();
                   PreparedStatement preparedStatement=connection.prepareStatement(sql);
               ){
                   for (int i = 0; i <batch.size() ; i++) {
                       preparedStatement.setString(1,batch.get(i).getTicketCode());
                       preparedStatement.setDate(2, (Date) batch.get(i).getTradeDate()); // 当前日期
                       preparedStatement.setString(3, batch.get(i).getTicketDc());
                       preparedStatement.setBigDecimal(4,batch.get(i).getPriceStart());
                       preparedStatement.setBigDecimal(5, batch.get(i).getPriceEnd());
                       preparedStatement.setBigDecimal(6, batch.get(i).getPriceMean());
                       preparedStatement.setBigDecimal(7, batch.get(i).getPriceMeanUp());
                       preparedStatement.setBigDecimal(8,batch.get(i).getPriceMeanDown());
                       preparedStatement.setBigDecimal(9, BigDecimal.ZERO);
                       preparedStatement.setBigDecimal(10, BigDecimal.ZERO);
                       preparedStatement.setBigDecimal(11, batch.get(i).getPriceStd());
                       preparedStatement.setBigDecimal(12,batch.get(i).getPriceMax());
                       preparedStatement.setBigDecimal(13, batch.get(i).getPriceMin());
                       preparedStatement.setBigDecimal(14, batch.get(i).getPriceRange());
                       preparedStatement.setBigDecimal(15, batch.get(i).getPriceSkewness());
                       preparedStatement.setBigDecimal(16,batch.get(i).getPriceKurtosis());
                       preparedStatement.setLong(17,batch.get(i).getVolumeTotal());
                       preparedStatement.setLong(18, batch.get(i).getVolumeUp());
                       preparedStatement.setLong(19,batch.get(i).getVolumeDown());
                       preparedStatement.setLong(20, 0L);
                       preparedStatement.setBigDecimal(21,batch.get(i).getVolumeD5Bucket());
                       preparedStatement.setBigDecimal(22, batch.get(i).getVolumeD4Bucket());
                       preparedStatement.setBigDecimal(23, batch.get(i).getVolumeD3Bucket());
                       preparedStatement.setBigDecimal(24, batch.get(i).getVolumeD2Bucket());
                       preparedStatement.setBigDecimal(25, batch.get(i).getVolumeD1Bucket());
                       preparedStatement.setBigDecimal(26, batch.get(i).getVolumeU1Bucket());
                       preparedStatement.setBigDecimal(27, batch.get(i).getVolumeU2Bucket());
                       preparedStatement.setBigDecimal(28,batch.get(i).getVolumeU3Bucket());
                       preparedStatement.setBigDecimal(29,batch.get(i).getVolumeU4Bucket());
                       preparedStatement.setBigDecimal(30, batch.get(i).getVolumeU5Bucket());
                       preparedStatement.addBatch();
                   }
                   int[] ints = preparedStatement.executeBatch();
                   batch.clear();
               }
       }
       }catch (Exception e){
           batch.clear();
           System.out.println("----------------insertdoris error---------"+e.getMessage());
       }

    }


    public static BigDecimal computeKurtosis(List<TicketPriceCount> ticks) {
        BigDecimal totalVolume = BigDecimal.ZERO;
        BigDecimal weightedSum = BigDecimal.ZERO;

        // 计算加权均值
        for (TicketPriceCount tpc : ticks) {
            BigDecimal vol = BigDecimal.valueOf(tpc.getVolumeSum());
            weightedSum = weightedSum.add(tpc.getPrice().multiply(vol));
            totalVolume = totalVolume.add(vol);
        }

        if (totalVolume.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }

        BigDecimal mean = weightedSum.divide(totalVolume, 8, RoundingMode.HALF_UP);

        BigDecimal sum2 = BigDecimal.ZERO; // 二阶距
        BigDecimal sum4 = BigDecimal.ZERO; // 四阶距

        for (TicketPriceCount tpc : ticks) {
            BigDecimal delta = tpc.getPrice().subtract(mean);
            BigDecimal vol = BigDecimal.valueOf(tpc.getVolumeSum());
            sum2 = sum2.add(delta.pow(2).multiply(vol));
            sum4 = sum4.add(delta.pow(4).multiply(vol));
        }
//标准差为0  无峰度
        if (sum2.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal stdDev = sqrt(sum2.divide(totalVolume.subtract(BigDecimal.ONE), 8, RoundingMode.HALF_UP),8);
        if (stdDev.compareTo(BigDecimal.ZERO) == 0) return BigDecimal.ZERO;

        BigDecimal numerator = sum4.divide(totalVolume, 8, RoundingMode.HALF_UP);
        BigDecimal kurtosis = numerator.divide(stdDev.pow(4), 8, RoundingMode.HALF_UP);

        // 超峰度 = 原始峰度 - 3
        return kurtosis.subtract(BigDecimal.valueOf(3));
    }


    public static BigDecimal weightedStdDev(List<TicketPriceCount> ticketPriceCounts) {
        if (ticketPriceCounts == null || ticketPriceCounts.isEmpty()) {
            throw new IllegalArgumentException("ticket list is null or empty");
        }

        BigDecimal totalVolume = BigDecimal.ZERO;
        BigDecimal weightedSum = BigDecimal.ZERO;

        // Step 1: 计算加权均值
        for (TicketPriceCount ticketPriceCount : ticketPriceCounts) {
            if (ticketPriceCount == null || ticketPriceCount.getPrice() == null || ticketPriceCount.getVolumeSum() == null) continue;
            if (ticketPriceCount.getVolumeSum() <= 0) continue;

            BigDecimal price = ticketPriceCount.getPrice();
            BigDecimal volume = BigDecimal.valueOf(ticketPriceCount.getVolumeSum());

            weightedSum = weightedSum.add(price.multiply(volume));
            totalVolume = totalVolume.add(volume);
        }

        if (totalVolume.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException("total volume is zero or all data is invalid");
        }

        BigDecimal weightedMean = weightedSum.divide(totalVolume, 10, RoundingMode.HALF_UP);

        // Step 2: 计算加权方差
        BigDecimal varianceSum = BigDecimal.ZERO;

        for (TicketPriceCount ticketPriceCount : ticketPriceCounts) {
            if (ticketPriceCount == null || ticketPriceCount.getPrice() == null || ticketPriceCount.getVolumeSum() == null) continue;
            if (ticketPriceCount.getVolumeSum() <= 0) continue;

            BigDecimal price = ticketPriceCount.getPrice();
            BigDecimal volume = BigDecimal.valueOf(ticketPriceCount.getVolumeSum());

            BigDecimal diff = price.subtract(weightedMean);
            varianceSum = varianceSum.add(diff.pow(2).multiply(volume));
        }
        //没有差价的时候标准差为0
        if (varianceSum.compareTo(BigDecimal.ZERO)==0) {
            return BigDecimal.ZERO;
        }

        BigDecimal variance = varianceSum.divide(totalVolume, 10, RoundingMode.HALF_UP);
        BigDecimal stdDev = sqrt(variance, 10); // 精确开平方

        return stdDev;
    }


    public static BigDecimal calculateSkewness(List<TicketPriceCount> data) {
        if (data == null || data.isEmpty()) return BigDecimal.ZERO;

        long totalVolumeSum = data.stream().mapToLong(tpc -> tpc.getVolumeSum()).sum();
        if (totalVolumeSum < 3) return BigDecimal.ZERO;

        // 计算加权平均（mean）
        BigDecimal weightedSum = data.stream()
                .map(tpc -> tpc.getPrice().multiply(BigDecimal.valueOf(tpc.getVolumeSum())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        BigDecimal mean = weightedSum.divide(BigDecimal.valueOf(totalVolumeSum), 8, RoundingMode.HALF_UP);

        // 计算标准差（std） 和 偏度部分
        BigDecimal stdNumerator = BigDecimal.ZERO;
        BigDecimal skewNumerator = BigDecimal.ZERO;

        for (TicketPriceCount tpc : data) {
            BigDecimal delta = tpc.getPrice().subtract(mean);
            BigDecimal delta2 = delta.pow(2);
            BigDecimal delta3 = delta.pow(3);

            BigDecimal vol = BigDecimal.valueOf(tpc.getVolumeSum());

            stdNumerator = stdNumerator.add(delta2.multiply(vol));
            skewNumerator = skewNumerator.add(delta3.multiply(vol));
        }
        //分布度为0 分布峰度为0
        if (stdNumerator.compareTo(BigDecimal.ZERO)==0) {
            return BigDecimal.ZERO;
        }

        BigDecimal stdDev = sqrt(stdNumerator.divide(BigDecimal.valueOf(totalVolumeSum - 1), 8, RoundingMode.HALF_UP),8);

        if (stdDev.compareTo(BigDecimal.ZERO) == 0) return BigDecimal.ZERO;

        BigDecimal skew = skewNumerator
                .divide(BigDecimal.valueOf(totalVolumeSum), 8, RoundingMode.HALF_UP)
                .divide(stdDev.pow(3), 8, RoundingMode.HALF_UP);

        // 样本修正因子 n/(n-1)(n-2)
        BigDecimal n = BigDecimal.valueOf(totalVolumeSum);
        BigDecimal correction = n.divide(
                (n.subtract(BigDecimal.ONE)).multiply(n.subtract(BigDecimal.valueOf(2))),
                8, RoundingMode.HALF_UP
        );

        return skew.multiply(correction);
    }
    // BigDecimal 精确开方实现（牛顿法）
    public static BigDecimal sqrt(BigDecimal value, int scale) {
        BigDecimal x = new BigDecimal(Math.sqrt(value.doubleValue()));
        BigDecimal TWO = BigDecimal.valueOf(2);
        for (int i = 0; i < 10; i++) {
            x = x.add(value.divide(x, scale, RoundingMode.HALF_UP)).divide(TWO, scale, RoundingMode.HALF_UP);
        }
        return x;
    }

    public static Map<String, BigDecimal> calculateVolumeBuckets(
            List<TicketPriceCount> sortedList,
            BigDecimal upLimitPrice,
            BigDecimal downLimitPrice,
            BigDecimal volumeSumAll) {
        Statement statement = null;
        String queryall = "SELECT *  FROM  ticket_price_count tpc  WHERE  ts_code ='" + sortedList.get(0).getOrderCode() + "' AND  trade_date ='" + sortedList.get(0).getOrderDate() + "'";
        try (Connection connection = DatabaseConnectionPoolNew.getConnection();
             PreparedStatement resultSet11 = connection.prepareStatement(queryall);) {
            List<TicketStkLimit> list = new ArrayList<>();
            boolean flag = true;
            statement = connection.createStatement();
            ResultSet resultSet = resultSet11.executeQuery();
            while (resultSet.next()) {
                flag = false;
                TicketStkLimit ticketStkLimit = new TicketStkLimit();
                ticketStkLimit.setTsCode(resultSet.getString("ts_code"));
                ticketStkLimit.setTradeDate(resultSet.getString("trade_date"));
                ticketStkLimit.setUpLimit(upLimitPrice);
                ticketStkLimit.setDownLimit(downLimitPrice);
                list.add(ticketStkLimit);
            }
            if (flag) {
                return null;
            }
            Map<String, BigDecimal> result = new LinkedHashMap<>();
            String[] bucketNames = {
                    "down5Bucket", "down4Bucket", "down3Bucket", "down2Bucket", "down1Bucket",
                    "up1Bucket", "up2Bucket", "up3Bucket", "up4Bucket", "up5Bucket"
            };

            for (String name : bucketNames) {
                result.put(name, BigDecimal.ZERO);
            }

            // 边界检查
            if (sortedList == null || sortedList.isEmpty()
                    || upLimitPrice == null || downLimitPrice == null
                    || volumeSumAll == null || volumeSumAll.compareTo(BigDecimal.ZERO) == 0) {
                return result;
            }

            // 计算区间步长
            BigDecimal interval = upLimitPrice.subtract(downLimitPrice).divide(BigDecimal.TEN, 10, RoundingMode.HALF_UP);

            // 统计各区间的 volumeSum
            for (TicketPriceCount item : sortedList) {
                if (item == null || item.getPrice() == null || item.getVolumeSum() == null) {
                    continue;
                }

                BigDecimal price = item.getPrice();
                BigDecimal volume = BigDecimal.valueOf(item.getVolumeSum());

                if (price.compareTo(downLimitPrice) < 0 || price.compareTo(upLimitPrice) > 0) {
                    continue; // 超出涨跌停范围
                }

                int index = price.subtract(downLimitPrice)
                        .divide(interval, 0, RoundingMode.DOWN)
                        .intValue();

                // 修正 index = 10 的情况（即精确为涨停价）
                if (index >= 10) index = 9;

                String bucketName = bucketNames[index];
                BigDecimal current = result.get(bucketName);
                result.put(bucketName, current.add(volume));
            }
            // 计算占比
            for (String bucket : bucketNames) {
                BigDecimal sum = result.get(bucket);
                BigDecimal ratio = sum.divide(volumeSumAll, 6, RoundingMode.HALF_UP); // 保留6位小数
                result.put(bucket, ratio);
            }
            return result;
        } catch (Exception e) {
            System.out.printf("eroor+"+e.getMessage());
            return null;
        }
    }


//    public static void shutdown() {
//        // 等待所有任务完成
//        try {
//            System.out.printf("-----end---------------");
//            executorService.shutdown();
//            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
//                executorService.shutdownNow();
//            }
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            executorService.shutdownNow();
//        }
//    }
public static void shutdown() {
    try {
        System.out.println("等待写线程完成任务...");
        executorService.shutdown();

        // 无限等待：等所有写入完成（更安全）
        if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
            System.err.println("等待超时，仍有任务未完成，谨慎执行 shutdownNow！");
            executorService.shutdownNow();
        }
    } catch (InterruptedException e) {
        System.err.println("主线程被中断，强制关闭线程池！");
        executorService.shutdownNow();
        Thread.currentThread().interrupt(); // 保留中断状态
    }
}

}

