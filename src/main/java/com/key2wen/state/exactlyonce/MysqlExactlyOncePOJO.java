package com.key2wen.state.exactlyonce;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MysqlExactlyOncePOJO {
    private String value;

    public MysqlExactlyOncePOJO(String value) {
        this.value = value;
    }
}