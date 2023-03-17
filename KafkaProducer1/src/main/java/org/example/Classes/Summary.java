package org.example.Classes;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@Entity
public class Summary {

    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    private Long id;
    private String message;

    @OneToOne
    @JoinColumn(name = "countries_id")
    private Countries countries;

    @OneToOne
    @JoinColumn(name = "global_id")
    private Global global;
    private Date date;


}
