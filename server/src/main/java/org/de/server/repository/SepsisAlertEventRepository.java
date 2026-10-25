package org.de.server.repository;

import org.de.server.entity.SepsisAlertEventEntity;
import org.de.server.entity.id.SepsisAlertEventId;
import org.de.server.enums.AlertStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface SepsisAlertEventRepository extends JpaRepository<SepsisAlertEventEntity, SepsisAlertEventId> {

    @Query(
            """
            SELECT a FROM SepsisAlertEventEntity a
            WHERE a.stayId = :stayId
              AND (:status IS NULL OR a.status = :status)
            ORDER BY a.id.alertTime DESC
            """
    )
    Page<SepsisAlertEventEntity> findAlertHistory(
            @Param("stayId") Long stayId,
            @Param("status") AlertStatus status,
            Pageable pageable
    );
}
