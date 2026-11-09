package practice.dtos;

import org.hibernate.query.Page;

import java.math.BigDecimal;
import java.util.List;

public record OrdenesDto(
        Long id,
        BigDecimal total,
        String nombreSucursal,
        List<ProductosDto> productosDtoList
) {
}
