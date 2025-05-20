package doris_flink.test00001;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
@Data
@RequiredArgsConstructor
public class StockTicket {
    public String orderCode;
    public String orderDate;
    public long tranId;
    public String orderTime;
    public String orderDc;
    public BigDecimal price;
    public long volume;
    public long saleVolume;
    public long buyVolume;
    public String type;
    public long saleOrderId;
    public BigDecimal saleOrderPrice;
    public long buyOrderId;
    public BigDecimal buyOrderPrice;





    // 构造函数、Getter 和 Setter 方法省略
}

