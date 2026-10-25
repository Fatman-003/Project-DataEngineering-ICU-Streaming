package org.de.server.repository;

import org.de.server.entity.VitalsSlidingWindow5mEntity;
import org.de.server.entity.id.VitalsSlidingWindow5mId;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface VitalsSlidingWindow5mRepository extends JpaRepository<VitalsSlidingWindow5mEntity, VitalsSlidingWindow5mId> {

    List<VitalsSlidingWindow5mEntity> findByIdStayIdOrderByIdWindowEndDesc(Long stayId, Pageable pageable);
}
