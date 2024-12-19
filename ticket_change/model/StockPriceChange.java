package model;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class StockPriceChange {
    private String orderCode;  // `order_code` CHAR(16)
    private Date orderDate;    // `order_date` DATE
    private String orderTime;  // `order_time` CHAR(32)
    private String orderDc;    // `order_dc` CHAR(32)
    private BigDecimal price;  // `price` DECIMAL(10,2)
    private Long volume;       // `volume` BIGINT
    private BigDecimal sum;    // `sum` DECIMAL(10,2)
    // 构造函数、Getter 和 Setter 方法省略


    // 添加交易记录
    public void addTransaction(StockTicket ticket) {
        // 这里可以根据 ticket 的类型来更新不同的值
        this.volume += ticket.getVolume();
        this.sum = this.sum.add(ticket.getPrice().multiply(BigDecimal.valueOf(ticket.getVolume())));
    }

    // 合并两个 StockPriceChange 对象
    public StockPriceChange merge(StockPriceChange other) {
        // 合并金额
        this.sum = this.sum.add(other.sum);
        // 合并交易量
        this.volume += other.volume;
        return this;
    }


}

