package org.example.Controllers;

import org.example.Repositories.GlobalRepository;
import org.springframework.beans.factory.annotation.Autowired;

import org.example.Classes.Countries;
import org.example.Repositories.CountriesRepository;


import javax.transaction.Transactional;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("countries")
public class CountriesController {

    @Autowired
    public CountriesRepository countriesRepository;

    @Autowired
    public GlobalRepository globalRepository;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Countries getCountryValues(String country){

    };

}