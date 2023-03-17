package org.example.Repositories;

import org.example.Classes.Countries;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.transaction.Transactional;
import java.util.List;

public interface CountriesRepository extends JpaRepository<Countries, Long> {



}
