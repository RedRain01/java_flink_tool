//import model.StockPriceChange;
//import model.StockTicket;
//import org.apache.doris.flink.cfg.DorisExecutionOptions;
//import org.apache.doris.flink.cfg.DorisOptions;
//import org.apache.doris.flink.cfg.DorisReadOptions;
//import org.apache.doris.flink.sink.DorisSink;
//import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
//import org.apache.doris.flink.source.DorisSource;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.RuntimeExecutionMode;
//
//import java.util.List;
//import java.util.ArrayList;
//import java.util.Properties;
//
//public class StockPriceChangeAggregation {
//
//    public static void main(String[] args) throws Exception {
//        // Flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);  // 设置合适的并行度
//
//        // 启用检查点
//        env.enableCheckpointing(10000);
//
//        // 设置为批处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//
//        // 配置 Doris 连接信息
//        String dorisUrl = "http://doris_host:8030";  // Doris FE HTTP API 地址
//        String dorisUser = "root";  // Doris 用户名
//        String dorisPassword = "password";  // Doris 密码
//        String dorisDatabase = "stock_data";  // Doris 数据库
//        String dorisTable = "ticket";  // Doris 表
//
//        // 获取订单代码和日期列表
//        List<Tuple2<String, String>> orderCodeDateList = getOrderCodeDateList();
//
//        // 遍历订单代码和日期列表
//        for (Tuple2<String, String> item : orderCodeDateList) {
//            String orderCode = item.f0;
//            String orderDate = item.f1;
//
//            // 构建 DorisSource 配置
//            DorisOptions dorisOptions = DorisOptions.builder()
//                    .setFenodes(dorisUrl)
//                    .setTableIdentifier(dorisDatabase + "." + dorisTable)
//                    .setUsername(dorisUser)
//                    .setPassword(dorisPassword)
//                    .build();
//
//            // 不再使用 DorisReadOptions 设置 WHERE 条件，而是直接通过 setQuery 来指定 SQL 查询
//            DorisReadOptions dorisReadOptions = DorisReadOptions.builder()
//                    .build();  // 如果没有其他特殊的读取配置，保留默认配置
//
//            DorisSource<StockTicket> dorisSource = DorisSource.<StockTicket>builder()
//                    .setDorisOptions(dorisOptions)
//                    .setDorisReadOptions(DorisReadOptions.builder()
//                            .setWhereClause("order_code = '" + orderCode + "' AND order_date = '" + orderDate + "'")  // 设置 WHERE 子句
//                            .build())
//                    .setDeserializer(new StockTicketDeserializationSchema())  // 使用自定义反序列化
//                    .build();
//
//            // 构建 DorisSource
//            DorisSource<StockTicket> dorisSource = DorisSource.<StockTicket>builder()
//                    .setDorisOptions(dorisOptions)
//                    .setDorisReadOptions(dorisReadOptions)
//                    .setQuery("SELECT * FROM ticket WHERE order_code = '" + orderCode + "' AND order_date = '" + orderDate + "' ORDER BY order_time ASC")  // 直接设置 SQL 查询
//                    .setDeserializer(new StockTicketDeserializationSchema())  // 使用自定义反序列化
//                    .build();
//
//            // 从 Doris 读取数据流
//            DataStream<StockTicket> ticketStream = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "Doris Source");
//
//            // 对数据按价格变化进行聚合
//            DataStream<StockPriceChange> aggregatedData = ticketStream
//                    .keyBy(ticket -> Tuple2.of(ticket.getOrderCode(), ticket.getOrderDate()))  // 按股票代码和日期分组
//                    .window(TumblingProcessingTimeWindows.of(Time.days(1)))  // 以天为窗口
//                    .aggregate(new AggregateFunction<StockTicket, StockPriceChange, StockPriceChange>() {
//
//                        @Override
//                        public StockPriceChange createAccumulator() {
//                            return new StockPriceChange();
//                        }
//
//                        @Override
//                        public StockPriceChange add(StockTicket value, StockPriceChange accumulator) {
//                            // 对每个交易记录进行聚合
//                            accumulator.addTransaction(value);
//                            return accumulator;
//                        }
//
//                        @Override
//                        public StockPriceChange getResult(StockPriceChange accumulator) {
//                            return accumulator;
//                        }
//
//                        @Override
//                        public StockPriceChange merge(StockPriceChange a, StockPriceChange b) {
//                            return a.merge(b);
//                        }
//                    });
//
//            // 配置 Doris Sink 写入聚合结果
//            DorisSink<String> dorisSink = DorisSink.<String>builder()
//                    .setDorisOptions(dorisOptions)
//                    .setDorisExecutionOptions(DorisExecutionOptions.builder()
//                            .setLabelPrefix("label-doris") // StreamLoad label 前缀
//                            .setDeletable(false)
//                            .setStreamLoadProp(new Properties()) // 可根据需要设置其他属性
//                            .build())
//                    .setSerializer(new SimpleStringSerializer()) // 序列化为字符串
//                    .build();
//
//            // 将聚合后的数据写入 Doris
//            aggregatedData.map((MapFunction<StockPriceChange, String>) stockPriceChange -> {
//                // 生成写入 Doris 的数据格式
//                return stockPriceChange.toString(); // 根据 StockPriceChange 需要自定义 toString 方法
//            }).sinkTo(dorisSink);
//        }
//
//        // 启动作业
//        env.execute("Stock Price Change Aggregation");
//    }
//
//    // 获取股票的 order_code 和 order_date 列表
//    private static List<Tuple2<String, String>> getOrderCodeDateList() {
//        // 示例返回静态列表
//        return List.of(
//                Tuple2.of("000021", "2024-01-01"),
//                Tuple2.of("000021", "2024-01-02"),
//                Tuple2.of("000021", "2024-01-03")
//        );
//    }
//
//    // 自定义反序列化 StockTicket
//    public static class StockTicketDeserializationSchema implements org.apache.flink.api.common.serialization.DeserializationSchema<StockTicket> {
//        @Override
//        public StockTicket deserialize(byte[] message) throws IOException {
//            // 假设 message 是 JSON 格式，使用 JSON 反序列化
//            String jsonString = new String(message);
//            ObjectMapper objectMapper = new ObjectMapper();
//            return objectMapper.readValue(jsonString, StockTicket.class);
//        }
//
//        @Override
//        public boolean isEndOfStream(StockTicket nextElement) {
//            return false;
//        }
//
//        @Override
//        public TypeI
