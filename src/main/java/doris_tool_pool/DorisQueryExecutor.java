package doris_tool_pool;

import doris_tool.model.TicketPriceCount;

import java.math.BigDecimal;
import java.sql.*;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DorisQueryExecutor {

    // 创建一个固定大小的线程池来执行每个SQL任务
    public static void executeQuery(String code,String date) {
        // 每个查询任务都交给线程池中的一个独立线程处理
        try {
            Statement statement = null;
            BigDecimal startPrice=BigDecimal.ZERO;
            BigDecimal endPrice=BigDecimal.ZERO;;
            String queryall="SELECT *  FROM  ticket_price_count tpc  WHERE  order_code ='"+code+"' AND  order_date ='"+date+"'";
            String queryStart="SELECT * FROM  ticket_new  WHERE  order_code ='"+code+"' AND  order_date ='"+date+"' AND  tran_id IS NOT NULL order by order_time ASC limit  1";
            String queryEnd="SELECT * FROM  ticket_new  WHERE  order_code ='"+code+"' AND  order_date ='"+date+"' AND  tran_id IS NOT NULL order by order_time desc limit  1";

            try(Connection connection = DatabaseConnectionPoolNew.getConnection();
                PreparedStatement resultSet11 = connection.prepareStatement(queryall);
                PreparedStatement resultSetStart11 = connection.prepareStatement(queryStart);
                PreparedStatement resultSetEnd11 = connection.prepareStatement(queryEnd);

            ) {
                List<TicketPriceCount> list=new ArrayList<>();
                statement = connection.createStatement();
                ResultSet resultSet=resultSet11.executeQuery();
                while (resultSet.next()) {
                    TicketPriceCount ticketPriceCount=new TicketPriceCount();
                    String name = resultSet.getString("order_code");
                    ticketPriceCount.setOrderCode(resultSet.getString("order_code"));
                    ticketPriceCount.setOrderDate(resultSet.getDate("order_date"));
                    ticketPriceCount.setPrice(resultSet.getBigDecimal("price"));
                    ticketPriceCount.setVolumeSum(resultSet.getLong("volume_sum"));
                    ticketPriceCount.setSum(resultSet.getBigDecimal("sum"));
                    list.add(ticketPriceCount);
                    // 假设这里只是打印处理结果，你可以根据实际情况修改
                }
                if (list.size()<1) {
                    System.out.println("----------nolist-----"+code+"----date---"+date);
                    return;
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
                String sql = "INSERT INTO ticket_up_down_test (" +
                        "order_code," +
                        " order_date," +
                        "start_price," +
                        "end_price," +
                        "next_price," +
                        "avg_price," +
                        "volume,"+
                        "all_volume," +
                        "volume_proportion, " +
                        "up_volume, " +
                        "up_sum, " +
                        "down_volume, " +
                        "down_sum, " +
                        "up_down, " +
                        "up_down_proportion, " +
                        "up_price, " +
                        "down_price, " +
                        "price_tendency, " +
                        "score, " +
                        "value1) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?);";

                // 创建 PreparedStatement 对象
                PreparedStatement preparedStatement=connection.prepareStatement(sql);

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

                preparedStatement.setString(1, sortedList.get(0).getOrderCode());
                preparedStatement.setDate(2, (Date) sortedList.get(0).getOrderDate()); // 当前日期
                preparedStatement.setBigDecimal(3, startPrice);
                preparedStatement.setBigDecimal(4, endPrice);
                preparedStatement.setBigDecimal(5, null);
                preparedStatement.setBigDecimal(6,evgPrice);
                preparedStatement.setLong(7, allReduce.getValue());
                preparedStatement.setLong(8, 0l);
                preparedStatement.setBigDecimal(9,null);
                preparedStatement.setLong(10, upReduce.getValue());
                preparedStatement.setBigDecimal(11, upReduce.getKey());
                preparedStatement.setLong(12, downReduce.getValue());
                preparedStatement.setBigDecimal(13, downReduce.getKey());
                preparedStatement.setString(14, upReduce.getKey().compareTo(downReduce.getKey())==1?"up":upReduce.getKey().compareTo(downReduce.getKey())==0?"balance":"down");
                preparedStatement.setBigDecimal(15, null);
                preparedStatement.setBigDecimal(16,evgUpPrice);
                preparedStatement.setBigDecimal(17,evgDownPrice);
                preparedStatement.setBigDecimal(18,evgPrice.divide(sortedList.get(0).getPrice(), 2, BigDecimal.ROUND_HALF_UP));
                preparedStatement.setLong(19, 0);
                preparedStatement.setString(20, null);
                // 执行插入操作
                int rowsAffected = preparedStatement.executeUpdate();

                //  connection.prepareStatement()

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("----------000---------"+e.getMessage());

            }
        }
        catch (Exception e) {
            System.out.println("----------222---------"+e.getMessage());
         }
    }
}

