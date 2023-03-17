package org.example.Repositories;

import org.example.Classes.Global;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.transaction.Transactional;

public interface GlobalRepository extends JpaRepository<Global, Long> {


}