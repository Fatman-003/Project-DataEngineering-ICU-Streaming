package org.de.server.repository;

import org.de.server.entity.IcuStayEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IcuStayRepository extends JpaRepository<IcuStayEntity, Long> {

    @Query(
            "SELECT i FROM IcuStayEntity i " +
            "JOIN FETCH i.patient p " +
            "JOIN FETCH i.admission a " +
            "WHERE i.stayId = :stayId"
    )
    Optional<IcuStayEntity> findByStayId(Long stayId);

    @Override
    @EntityGraph(attributePaths = {
            "patient",
            "admission"
    })
    Page<IcuStayEntity> findAll(Pageable pageable);
}
