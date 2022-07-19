package com.github.zag13.tmall;

import lombok.*;
import org.apache.commons.lang3.time.FastDateFormat;

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TmallOrder {
    private String orderId;
    private Integer userId;
    private Double orderAmount;
    private String orderTime;
    private String category;

    @Override
    public String toString() {
        return orderId + ", " + userId + ", " + orderAmount + ", " + category + ", " + orderTime;
    }

    public long getEventTime() {
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS");
        long eventTime = 0;
        try {
            eventTime = format.parse(this.getOrderTime()).getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return eventTime;
    }
}