package model;

import lombok.Data;

import java.math.BigDecimal;
@Data
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

    public StockTicket(String orderCode, String orderDate, String orderTime, String orderDc, Double price, Long volume, Double sum) {
    }

    // 构造函数、Getter 和 Setter 方法省略
}

