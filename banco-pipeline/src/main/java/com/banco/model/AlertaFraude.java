package com.banco.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlertaFraude {
    private String usuario;
    private Double monto;
    private String motivo;
    private String severidad; // "ALTA", "MEDIA"
}
