package doris_flink.test00001;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StockPriceChangeAggregation0001{

    public static void main(String[] args) throws Exception {
        // 获取 Flink 执行环境
        // 创建一个 Configuration 对象
        Configuration config = new Configuration();

        // 设置每个 TaskManager 的任务槽数量
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1); // 设置为 4 个任务槽

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1); // 设置并行度
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 从 Doris 获取所有的 order_code 和 order_date 列表
        List<Tuple2<String, String>> orderCodeDateList = getOrderCodeDateList();

        DorisOptions.Builder builder = DorisOptions.builder();
        builder.setFenodes("192.168.0.104:8030")
                .setTableIdentifier("demo.ticket")  // 从 ticket_test 表读取数据
                .setUsername("root")
                .setPassword("why123");

        DorisOptions.Builder builder3 = DorisOptions.builder();
        builder3.setFenodes("192.168.0.104:8030")
                .setTableIdentifier("demo.ticket_change")  // 从 ticket_test 表读取数据
                .setUsername("root")
                .setPassword("why123");

        DorisReadOptions.Builder builder2 = DorisReadOptions.builder();
        builder2.setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);



        for (int i = 0; i < orderCodeDateList.size(); i++) {
            System.out.println("------ALL---------------"+orderCodeDateList.size());
            System.out.println("------ALL---------------"+orderCodeDateList.size());
            System.out.println("------ING---------------"+i);
            System.out.println("------ING---------------"+i);
            String orderCode = orderCodeDateList.get(i).f0;
            String orderDate = orderCodeDateList.get(i).f1;
            // 定义一个总的数据流
            DataStream<StockTicket> aggregatedStream = null;
            // 获取订单代码和日期列表
            // 构建 DorisSource 配置 (读取 ticket_test 表)


            // DorisSource 配置
            // String filterQuery = String.format("`order_code` = '%s' AND `order_date` = '%s' AND `tran_id` IS NOT NULL",
            String filterQuery = "`order_code` = '"+ orderCode +"' AND `order_date` = '"+orderDate+"' AND  `tran_id`  IS NOT NULL";
            builder2.setFilterQuery(filterQuery);
            DorisSource<StockTicket> dorisSource = DorisSource.<StockTicket>builder()
                    .setDorisOptions(builder.build())
                    .setDorisReadOptions(builder2.build())
                    .setDeserializer(new StockTicketDeserializationSchema())  // 使用自定义反序列化模式
                    .build();

            // 从 Doris 读取数据流
            DataStream<StockTicket> ticketStream = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "Doris Source");
            // 使用 KeyedProcessFunction 来跟踪价格变化
            DataStream<StockPriceChange> aggregatedData = ticketStream
                    .keyBy(new KeySelector<StockTicket, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, String> getKey(StockTicket ticket) throws Exception {
                            return Tuple2.of(ticket.getOrderCode(), ticket.getOrderDate());
                        }
                    })
                    .process(new PriceChangeAggregationFunction());

            DorisOptions options =
                    DorisOptions.builder()
                            .setUsername("root")
                            .setPassword("why123")
                            .setFenodes("127.0.0.1:8030")
                            .setTableIdentifier("demo.ticket_changes")
                            .build();


            // 配置 Doris Sink 写入 ticket_change 表
            DorisSink<String> dorisSink = DorisSink.<String>builder()
                    .setDorisOptions(options)
                    .setDorisExecutionOptions(DorisExecutionOptions.builder()
                            .setLabelPrefix("label-doris")
                            .setDeletable(false)
                            .setStreamLoadProp(new Properties())
                            .build())
                    .setSerializer(new SimpleStringSerializer())  // 序列化为字符串
                    .build();

            // 将聚合后的数据写入 Doris 的 ticket_change 表
            aggregatedData.map((MapFunction<StockPriceChange, String>) stockPriceChange -> {
                return stockPriceChange.toString();  // 自定义转换为字符串
            }).sinkTo(dorisSink);
            env.execute("Stock Price Change Aggregation+"+orderCode+"----"+orderDate);

        }
        // 启动作业

    }

    // 获取股票的 order_code 和 order_date 列表
    private static List<Tuple2<String, String>> getOrderCodeDateList() {
        // Doris 的 JDBC URL，使用 MySQL 协议
        String url = "jdbc:mysql://192.168.0.104:9030/demo"; // 替换为您的 Doris 地址和数据库名
        String user = "root"; // Doris 用户名
        String password = "why123"; // Doris 密码

        // 查询语句
        String query = "SELECT order_code, order_date FROM ticket_date where  order_date='2024-11-27' ";

        // 用于存储结果的列表
        List<Tuple2<String, String>> orderDataList = new ArrayList<>();

        try (
                // 创建数据库连接
                Connection connection = DriverManager.getConnection(url, user, password);
                // 创建 PreparedStatement
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                // 执行查询并获取结果集
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            // 遍历结果集
            while (resultSet.next()) {
                String orderCode = resultSet.getString("order_code");
                String orderDate = resultSet.getString("order_date");
                // 将结果存入列表
                orderDataList.add(Tuple2.of(orderCode, orderDate));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return orderDataList;


//        return List.of(
//                Tuple2.of("000488", "2024-01-03"),
//                Tuple2.of("000488", "2024-01-03"),
//                Tuple2.of("000488", "2024-01-03")
//        );
    }


    // KeyedProcessFunction 用于处理价格变化并触发聚合
    public static class PriceChangeAggregationFunction extends KeyedProcessFunction<Tuple2<String, String>, StockTicket, StockPriceChange> {

        // 用于存储上一个价格状态
        private ValueState<BigDecimal> lastPriceState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化 ValueState
            ValueStateDescriptor<BigDecimal> descriptor = new ValueStateDescriptor<>(
                    "lastPrice", BigDecimal.class);
            lastPriceState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(StockTicket value, Context ctx, Collector<StockPriceChange> out) throws Exception {
            // 获取上一个价格
            BigDecimal lastPrice = lastPriceState.value();

            // 如果当前价格与上一个价格不相同，触发聚合
            if (lastPrice == null || !lastPrice.equals(value.getPrice())) {
                // 价格发生变化，创建一个新的聚合结果
                StockPriceChange stockPriceChange = new StockPriceChange();
                stockPriceChange.addTransaction(value);  // 添加当前交易数据

                // 发出聚合结果
                out.collect(stockPriceChange);
            }

            // 更新上一个价格为当前价格
            lastPriceState.update(value.getPrice());
        }
    }



}
