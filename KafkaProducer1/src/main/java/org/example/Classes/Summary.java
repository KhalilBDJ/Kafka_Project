package org.example.Classes;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@AllArgsConstructor
public class Summary {
    private String message;
    private Countries countries;
    private Global global;
    private Date date;
}
