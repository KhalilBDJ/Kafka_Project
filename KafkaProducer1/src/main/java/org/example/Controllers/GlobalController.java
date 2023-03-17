/*
package org.example.Controllers;

import org.example.Repositories.CountriesRepository;
import org.springframework.beans.factory.annotation.Autowired;

import org.example.Classes.Global;
import org.example.Repositories.GlobalRepository;
import javax.ws.rs.core.MediaType;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.File;

@Path("global")
public class GlobalController {

    @Autowired
    private CountriesRepository countriesRepository;

    @Autowired
    private GlobalRepository globalRepository;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Global getGlobalValues(){
        return globalRepository.findOne(1L);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Integer getConfirmedAvg(Long id){

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Integer getDeathsAvg(Long id){

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Integer getCountryDeathsPercent(Long id){

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public File export(Long id){

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String help(Long id){

    }

}*/
