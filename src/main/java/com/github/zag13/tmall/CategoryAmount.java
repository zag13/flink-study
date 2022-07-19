package com.github.zag13.tmall;

import lombok.*;

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class CategoryAmount {
    private String category;
    private Double totalAmount;
    private String computeDateTime;

    @Override
    public String toString() {
        return "[" + computeDateTime + "]: " + category + " = " + totalAmount;
    }

    public String toContent() {
        return category + " = " + totalAmount;
    }
}