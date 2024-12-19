package model;

import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import model.StockTicket;

import java.util.List;

public class StockTicketDeserializationSchema implements DorisDeserializationSchema<StockTicket> {

    @Override
    public void deserialize(List<?> list, Collector<StockTicket> collector) throws Exception {
        // 假设数据列表中的每个元素顺序对应数据库字段
        String orderCode = list.get(0) != null ? list.get(0).toString() : null;
        String orderDate = list.get(1) != null ? list.get(1).toString() : null;
        String orderTime = list.get(2) != null ? list.get(2).toString() : null;
        String orderDc = list.get(3) != null ? list.get(3).toString() : null;

        // 转换其他字段
        Double price = list.get(4) != null ? Double.parseDouble(list.get(4).toString()) : 0.0;
        Long volume = list.get(5) != null ? Long.parseLong(list.get(5).toString()) : 0L;
        Double sum = list.get(6) != null ? Double.parseDouble(list.get(6).toString()) : 0.0;

        // 创建StockTicket对象
        StockTicket stockTicket = new StockTicket(orderCode, orderDate, orderTime, orderDc, price, volume, sum);

        // 将对象添加到collector中
        collector.collect(stockTicket);
    }

    @Override
    public TypeInformation<StockTicket> getProducedType() {
        return TypeInformation.of(StockTicket.class);
    }
}
