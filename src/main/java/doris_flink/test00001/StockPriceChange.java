package doris_flink.test00001;

import lombok.Data;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class StockPriceChange {
    private String orderCode;  // `order_code` CHAR(16)
    private Date orderDate;    // `order_date` DATE
    private String orderTime;  // `order_time` CHAR(32)
    private String orderDc;    // `order_dc` CHAR(32)
    private BigDecimal price;  // `price` DECIMAL(10,2)
    private Long volume=0L;       // `volume` BIGINT
    private BigDecimal sum=BigDecimal.ZERO;    // `sum` DECIMAL(10,2)
    // 构造函数、Getter 和 Setter 方法省略


    // 添加交易记录
    public void addTransaction(StockTicket ticket) {
        // 这里可以根据 ticket 的类型来更新不同的值
        try {
            // 定义日期格式
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            try {
                this.orderDate =  dateFormat.parse(ticket.getOrderDate());
            } catch (Exception e) {
                this.orderDate = new Date();
                // 捕获解析异常
            }
            this.orderCode = ticket.getOrderCode();
            this.orderTime = ticket.getOrderTime();
            this.orderDc = ticket.getOrderDc();
            this.price = ticket.getPrice();
            this.volume += ticket.getVolume();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.sum = this.sum.add(ticket.getPrice().multiply(BigDecimal.valueOf(ticket.getVolume())));
    }



    // 合并两个 StockPriceChange 对象
    public StockPriceChange merge(StockPriceChange other) {
        // 合并金额
        this.orderTime = other.orderTime;
        this.sum = this.sum.add(other.sum);
        // 合并交易量
        this.volume += other.volume;
        return this;
    }
    @Override
    public String toString() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        // 格式化 Date 对象为字符串
        String formattedDate = formatter.format(orderDate);
        return String.join("\t",
                orderCode,
                formattedDate,                // 格式化 Date 为字符串
                orderTime,
                orderDc,
                price.toPlainString(),               // 格式化 BigDecimal
                String.valueOf(volume),              // 转换 Long 为字符串
                sum.toPlainString()                  // 格式化 BigDecimal
        );
    }


}

