package doris_tool.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;


@AllArgsConstructor
@Data
@RequiredArgsConstructor
public class TicketPriceCount {
    private String orderCode;       // `order_code` char(16)
    private Date orderDate;         // `order_date` date
    private BigDecimal price;       // `price` decimal(10,2)
    private Long volumeSum;         // `volume_sum` bigint
    private BigDecimal sum;         // `sum` decimal(18,2)
}
