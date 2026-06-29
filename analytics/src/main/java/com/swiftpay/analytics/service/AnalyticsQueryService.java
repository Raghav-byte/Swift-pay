package com.swiftpay.analytics.service;

import com.swiftpay.analytics.dto.AnalyticsSummaryDTO;
import com.swiftpay.analytics.dto.VolumeByMinuteDTO;
import com.swiftpay.analytics.exception.SwiftPayException;
import com.swiftpay.analytics.repository.AnalyticsEventRepository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class AnalyticsQueryService {

    private final AnalyticsEventRepository analyticsEventRepository;

    public AnalyticsQueryService(AnalyticsEventRepository analyticsEventRepository) {
        this.analyticsEventRepository = analyticsEventRepository;
    }

    public AnalyticsSummaryDTO getSummary(String currency) throws SwiftPayException {
        try {
            List<Object[]> rows = analyticsEventRepository.getSummary(currency);
            Object[] row = firstRow(rows);
            long totalTransactions = ((Number) row[0]).longValue();
            BigDecimal totalVolume = toBigDecimal(row[1]);
            return new AnalyticsSummaryDTO(totalTransactions, totalVolume, currency);
        } catch (Exception e) {
            throw new SwiftPayException(500, "ANALYTICS_QUERY_FAILED", "Failed to fetch summary", e);
        }
    }

    public List<VolumeByMinuteDTO> getVolumeByMinute(int minutes) throws SwiftPayException {
        if (minutes < 1 || minutes > 1440) {
            throw new SwiftPayException(400, "INVALID_MINUTES", "minutes must be between 1 and 1440");
        }
        try {
            List<Object[]> rows = analyticsEventRepository.getVolumeByMinute(minutes);
            return rows.stream()
                    .map(row -> {
                        Object[] cols = unwrapRow(row);
                        return new VolumeByMinuteDTO(
                                cols[0].toString(),
                                ((Number) cols[1]).longValue(),
                                toBigDecimal(cols[2]));
                    })
                    .toList();
        } catch (Exception e) {
            throw new SwiftPayException(500, "ANALYTICS_QUERY_FAILED", "Failed to fetch volume", e);
        }
    }

    private static Object[] firstRow(List<Object[]> rows) {
        if (rows == null || rows.isEmpty()) {
            return new Object[] {0L, BigDecimal.ZERO};
        }
        return unwrapRow(rows.get(0));
    }

    /** Spring Data may nest column values in a single-element outer array. */
    private static Object[] unwrapRow(Object[] row) {
        if (row.length == 1 && row[0] instanceof Object[] nested) {
            return nested;
        }
        return row;
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return BigDecimal.ZERO;
        }
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        return new BigDecimal(value.toString());
    }
}
