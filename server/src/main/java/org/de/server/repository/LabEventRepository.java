package org.de.server.repository;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.LabResultDto;
import org.de.server.enums.LabFlag;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class LabEventRepository {

    private static final String BASE_QUERY = """
            SELECT
                l.ctid::text AS lab_id,
                i.stay_id AS stay_id,
                l.charttime AS collected_at,
                l.itemid AS item_id,
                COALESCE(d.label, 'Unknown test') AS name,
                d.category AS category,
                l.valuenum AS value,
                l.value AS raw_value,
                l.valueuom AS unit,
                NULL::double precision AS reference_low,
                NULL::double precision AS reference_high,
                CASE
                    WHEN COALESCE(l.flags, '') = '' THEN 'UNKNOWN'
                    ELSE UPPER(l.flags)
                END AS flag
            FROM labevents l
            JOIN icustays i ON l.hadm_id = i.hadm_id
            LEFT JOIN d_labitems d ON d.itemid = l.itemid
            WHERE i.stay_id = :stayId
              AND (:fromTime::timestamp IS NULL OR l.charttime >= :fromTime)
              AND (:toTime::timestamp IS NULL OR l.charttime <= :toTime)
              AND (:abnormalOnly = false OR COALESCE(l.flags, '') <> '')
            ORDER BY l.charttime DESC
            LIMIT :limit
            """;

    private static final String COUNT_QUERY = """
            SELECT COUNT(*)
            FROM labevents l
            JOIN icustays i ON l.hadm_id = i.hadm_id
            WHERE i.stay_id = :stayId
              AND (:fromTime::timestamp IS NULL OR l.charttime >= :fromTime)
              AND (:toTime::timestamp IS NULL OR l.charttime <= :toTime)
              AND (:abnormalOnly = false OR COALESCE(l.flags, '') <> '')
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public PageResult<LabResultDto> findLabHistory(
            Long stayId,
            OffsetDateTime from,
            OffsetDateTime to,
            boolean abnormalOnly,
            int limit
    ) {
        var params = new MapSqlParameterSource()
                .addValue("stayId", stayId)
                .addValue("fromTime", toLocalDateTime(from))
                .addValue("toTime", toLocalDateTime(to))
                .addValue("abnormalOnly", abnormalOnly)
                .addValue("limit", limit);

        Long total = jdbcTemplate.queryForObject(COUNT_QUERY, params, Long.class);
        List<LabResultDto> results = jdbcTemplate.query(
                BASE_QUERY,
                params,
                (rs, rowNum) -> new LabResultDto(
                        rs.getString("lab_id"),
                        rs.getLong("stay_id"),
                        toLocalDateTime(rs.getTimestamp("collected_at")),
                        (Integer) rs.getObject("item_id"),
                        rs.getString("name"),
                        rs.getString("category"),
                        (Double) rs.getObject("value"),
                        rs.getString("raw_value"),
                        rs.getString("unit"),
                        (Double) rs.getObject("reference_low"),
                        (Double) rs.getObject("reference_high"),
                        toFlag(rs.getString("flag"))
                )
        );

        return new PageResult<>(results, total == null ? 0 : total);
    }

    public record PageResult<T>(
            List<T> content,
            long totalElements
    ) {
    }

    private static LocalDateTime toLocalDateTime(OffsetDateTime value) {
        return value == null ? null : value.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private static LocalDateTime toLocalDateTime(Timestamp value) {
        return value == null ? null : value.toLocalDateTime();
    }

    private static LabFlag toFlag(String value) {
        try {
            return LabFlag.fromValue(value);
        } catch (IllegalArgumentException | NullPointerException exception) {
            return LabFlag.UNKNOWN;
        }
    }
}
