package doris_tool_pool.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@AllArgsConstructor
@Data
@RequiredArgsConstructor
public class TicketUpDown {
    private String orderCode;        // `order_code` char(16)
    private Date orderDate;          // `order_date` date
    private BigDecimal startPrice;   // `start_price` decimal(10,2)
    private BigDecimal endPrice;     // `end_price` decimal(10,2)
    private BigDecimal nextPrice;    // `next_price` decimal(10,2)
    private BigDecimal avgPrice;     // `avg_price` decimal(10,2)
    private Long volume;             // `volume` bigint
    private Long allVolume;          // `all_volume` bigint
    private BigDecimal volumeProportion; // `volume_proportion` decimal(4,2)
    private Long upVolume;           // `up_volume` bigint
    private BigDecimal upSum;        // `up_sum` decimal(18,2)
    private Long downVolume;         // `down_volume` bigint
    private BigDecimal downSum;      // `down_sum` decimal(18,2)
    private String upDown;           // `up_down` char(16)
    private BigDecimal upDownProportion; // `up_down_proportion` decimal(2,2)
    private Long score;              // `score` bigint
    private String value1;           // `value1` char(16)

}
