package org.example.Repositories;

import org.example.Classes.Global;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.transaction.Transactional;

public interface GlobalRepository extends JpaRepository<Global, Long> {

    @Transactional
    public void getGlobalValues(Long id);

    @Transactional
    public void export(Long id);

    @Transactional
    public void help(Long id);

}