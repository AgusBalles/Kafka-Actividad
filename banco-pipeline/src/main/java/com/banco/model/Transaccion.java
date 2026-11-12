package com.banco.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaccion {
    private String usuario;
    private Double monto;
    private String origen; // "cajero", "web", "app"
    private Long timestamp;
    private Double montoUsd; // Lo completa el paso de enriquecimiento del pipeline
}
