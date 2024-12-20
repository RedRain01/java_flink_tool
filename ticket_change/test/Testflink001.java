package test;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class Testflink001 {
    public static void main(String[] args) {
            DorisOptions.Builder builder = DorisOptions.builder();
            builder.setFenodes("192.168.0.104:9060")
                    .setTableIdentifier("demo.ticket_test")
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

            DorisSource<List<?>> dorisSource =
                    DorisSource.<List<?>>builder()
                            .setDorisOptions(builder.build())
                            .setDorisReadOptions(builder2.build())
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

        try {
            env.execute("Flink44 doris source test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
