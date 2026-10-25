package org.de.server.kafka.message;

/**
 * Mã lỗi của một message bị đẩy vào DLQ.
 *
 * <p>Thêm mã mới làm tăng {@code minor} của {@code schema_version}.
 */
public enum ErrorCode {

    JSON_PARSE_ERROR(ErrorStage.DESERIALIZATION, false),
    INVALID_UTF8(ErrorStage.DESERIALIZATION, false),

    SCHEMA_VERSION_UNSUPPORTED(ErrorStage.VALIDATION, false),
    MISSING_REQUIRED_FIELD(ErrorStage.VALIDATION, false),
    INVALID_FIELD_TYPE(ErrorStage.VALIDATION, false),
    INVALID_ENUM_VALUE(ErrorStage.VALIDATION, false),
    INVALID_TIMESTAMP(ErrorStage.VALIDATION, false),
    VALUE_OUT_OF_RANGE(ErrorStage.VALIDATION, false),
    EVENT_TYPE_MISMATCH(ErrorStage.VALIDATION, false),

    IDEMPOTENCY_CONFLICT(ErrorStage.PROCESSING, false),
    ALERT_NOT_FOUND(ErrorStage.PROCESSING, false),

    REFERENCE_NOT_FOUND(ErrorStage.STORAGE, true),
    DB_WRITE_ERROR(ErrorStage.STORAGE, true),
    DB_CONSTRAINT_ERROR(ErrorStage.STORAGE, false),

    PROCESSING_ERROR(ErrorStage.PROCESSING, true),
    UNKNOWN_ERROR(ErrorStage.PROCESSING, true);

    private final ErrorStage stage;
    private final boolean retryable;

    ErrorCode(ErrorStage stage, boolean retryable) {
        this.stage = stage;
        this.retryable = retryable;
    }

    public ErrorStage stage() {
        return stage;
    }

    public boolean retryable() {
        return retryable;
    }
}
