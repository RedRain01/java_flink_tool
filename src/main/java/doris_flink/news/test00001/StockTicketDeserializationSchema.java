package doris_flink.news.test00001;

import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;

public class StockTicketDeserializationSchema implements DorisDeserializationSchema<StockTicket> {

    @Override
    public void deserialize(List<?> list, Collector<StockTicket> collector) throws Exception {
        // 创建StockTicket对象
        StockTicket stockTicket = new StockTicket(
                list.get(0).toString(),
                list.get(1).toString(),
                (long) list.get(2),
                list.get(3).toString(),
                list.get(4).toString(),
                (BigDecimal) list.get(5),
                (long) list.get(6),
                (long) list.get(7),
                (long) list.get(8),
                (String) list.get(9),
                (long) list.get(10),
                (BigDecimal) list.get(11),
                (long) list.get(12),
                (BigDecimal) list.get(13));


        // 将对象添加到collector中
        collector.collect(stockTicket);
    }

    @Override
    public TypeInformation<StockTicket> getProducedType() {
        return TypeInformation.of(StockTicket.class);
    }
}
