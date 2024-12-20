package model;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

public class StockPriceChangeAggregation123 {
    public static DorisReadOptions buildDorisReadOptions() {
        return dorisReadOptionsBuilder().build();
    }

    public static DorisReadOptions.Builder dorisReadOptionsBuilder() {
        DorisReadOptions.Builder builder = DorisReadOptions.builder();
        builder.setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);
        return builder;
    }

    public static DorisOptions buildDorisOptions() {
        DorisOptions.Builder builder = DorisOptions.builder();
        builder.setFenodes("192.168.0.104:8030")
                .setTableIdentifier("demo.ticket_test")
                .setUsername("root")
                .setPassword("why123");
        return builder.build();
    }
    public static void main(String[] args) throws Exception {
        DorisSource<List<?>> dorisSource =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(buildDorisOptions())
                        .setDorisReadOptions(buildDorisReadOptions())
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ServiceLoader<PipelineExecutorFactory> loader =
                ServiceLoader.load(PipelineExecutorFactory.class);
        final Iterator<PipelineExecutorFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            try {
                final PipelineExecutorFactory factory = factories.next();
            } catch (Throwable e) {
                if (e.getCause() instanceof NoClassDefFoundError) {
                } else {
                    throw e;
                }
            }
        }
        env.setParallelism(1);



        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris Source")
                .addSink(new PrintSinkFunction<>());
        env.execute("Flink doris source 11111test");
    }


}
