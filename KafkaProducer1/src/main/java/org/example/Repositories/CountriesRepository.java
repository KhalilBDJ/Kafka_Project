package org.example.Repositories;

import org.example.Classes.Countries;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.transaction.Transactional;
import java.util.List;

public interface CountriesRepository extends JpaRepository<Countries, Long> {

    @Transactional
    public void getCountryValues(Long id);

    @Transactional
    public void getConfirmedAvg(Long id);

    @Transactional
    public void getDeathsAvg(Long id);

    @Transactional
    public void getCountryDeathsPercent(Long id);

}