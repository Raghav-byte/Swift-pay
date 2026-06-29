package com.swiftpay.analytics.repository;

import com.swiftpay.analytics.entity.AnalyticsEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface AnalyticsEventRepository extends JpaRepository<AnalyticsEventEntity, UUID> {

    @Query(value = """
            SELECT COUNT(*) as total_count,
                   COALESCE(SUM(amount), 0) as total_volume
            FROM sp_analytics_events
            WHERE currency = :currency
            """, nativeQuery = true)
    List<Object[]> getSummary(@Param("currency") String currency);

    @Query(value = """
            SELECT date_trunc('minute', completed_at) AS minute,
                   COUNT(*) AS count,
                   SUM(amount) AS total_amount
            FROM sp_analytics_events
            WHERE completed_at >= NOW() - INTERVAL '1 minute' * :minutes
            GROUP BY date_trunc('minute', completed_at)
            ORDER BY minute DESC
            """, nativeQuery = true)
    List<Object[]> getVolumeByMinute(@Param("minutes") int minutes);
}
