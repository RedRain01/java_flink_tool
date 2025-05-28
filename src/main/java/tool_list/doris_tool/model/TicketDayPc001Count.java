package tool_list.doris_tool.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;


@AllArgsConstructor
@Data
@RequiredArgsConstructor
public class TicketDayPc001Count {
    private String ticketCode;
    private Date tradeDate;
    private String ticketDc;
    private BigDecimal priceStart;
    private BigDecimal priceEnd;
    private BigDecimal priceMean;
    private BigDecimal priceMeanUp;
    private BigDecimal priceMeanDown;
    private BigDecimal priceOpen30Mean;
    private BigDecimal priceMedian;
    private BigDecimal priceStd;
    private BigDecimal priceMax;
    private BigDecimal priceMin;
    private BigDecimal priceRange;
    private BigDecimal priceSkewness;
    private BigDecimal priceKurtosis;
    private Long volumeTotal;
    private Long volumeUp;
    private Long volumeDown;
    private Long volumeOpen30min;
    private BigDecimal volumeU1Bucket;
    private BigDecimal volumeU2Bucket;
    private BigDecimal volumeU3Bucket;
    private BigDecimal volumeU4Bucket;
    private BigDecimal volumeU5Bucket;
    private BigDecimal volumeD1Bucket;
    private BigDecimal volumeD2Bucket;
    private BigDecimal volumeD3Bucket;
    private BigDecimal volumeD4Bucket;
    private BigDecimal volumeD5Bucket;       // `sum` decimal(18,2)
}
