package com.alibaba.datax.plugin.reader.cachereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum CacheReaderErrorCode implements ErrorCode {
    ;

    private final String code;
    private final String description;

    private CacheReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
