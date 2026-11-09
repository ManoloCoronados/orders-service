package practice.dtos;

import java.math.BigDecimal;

public record ProductosDto(
        Long id,
        String codigo,
        String descripcion,
        BigDecimal precio,
        Long version
) {
}
