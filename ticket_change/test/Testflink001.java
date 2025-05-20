package test;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

public class Testflink001 {
    public static void main(String[] args) {
        // Configure Doris options
        DorisOptions.Builder builder = DorisOptions.builder();
        builder.setFenodes("192.168.0.104:8030")
                .setTableIdentifier("demo.ticket_test")
                .setUsername("root")
                .setPassword("why123");

        String filterQuery = " order_code = 000058 AND order_date = 2024-01-03";

        DorisReadOptions.Builder builder2 = DorisReadOptions.builder();
        builder2.setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
             //   .setFilterQuery(filterQuery)
                .setRequestTabletSize(1024 * 1024);

        DorisSource<List<?>> dorisSource =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(builder.build())
                        .setDorisReadOptions(builder2.build())
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Print each row of data
        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "Doris Source")
                .addSink(new SinkFunction<List<?>>() {
                    @Override
                    public void invoke(List<?> value, Context context) {
                        System.out.println("Row: " + value);
                    }
                });

        try {
            env.execute("Flink Doris Source Row Print Test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
