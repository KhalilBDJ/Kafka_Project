package org.example.Repositories;

import org.example.Classes.Global;
import org.example.Classes.Summary;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SummaryRepository extends JpaRepository<Summary, Long> {
}
