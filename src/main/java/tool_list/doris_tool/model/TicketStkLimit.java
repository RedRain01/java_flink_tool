package tool_list.doris_tool.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;


@AllArgsConstructor
@Data
@RequiredArgsConstructor
public class TicketStkLimit {
    private String tsCode;       // `order_code` char(16)
    private String tradeDate;         // `order_date` date
    private BigDecimal preClose;       // `price` decimal(10,2)
    private BigDecimal upLimit;         // `volume_sum` bigint
    private BigDecimal downLimit;         // `sum` decimal(18,2)
}
