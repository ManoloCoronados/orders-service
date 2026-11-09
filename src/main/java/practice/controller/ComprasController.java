package practice.controller;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import practice.dtos.PaginaOrdenesDto;
import practice.service.MicroServicioService;
import practice.dtos.OrdenesDto;
import practice.dtos.ProductosDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.data.domain.Pageable;

@RestController
@RequestMapping("/api/sucursal")
@RequiredArgsConstructor
public class ComprasController {

    private final MicroServicioService microServicioService;

    @PostMapping("/registrar")
    public ResponseEntity <Long> registrarCompra (@Valid @RequestBody OrdenesDto ordenesDto) {

        Long resultado = microServicioService.guardarOrden(ordenesDto);
        return ResponseEntity.status(HttpStatus.CREATED).body(resultado);
    }


    @GetMapping("/obtener/{id}")
    public ResponseEntity<OrdenesDto> obtenerCompra (@PathVariable Long id){
        OrdenesDto ordenesDto = microServicioService.obtenerOrden(id);
        return ResponseEntity.ok(ordenesDto);
    }


    /**
     * Endpoint donde Implementaremos Optimistic
     */
    @PutMapping("/actualizar")
    public ResponseEntity<ProductosDto> actualizarCompra (@RequestParam String codigo,
    @RequestParam BigDecimal precio, @RequestParam Long version){
        ProductosDto productosDto = microServicioService.actualizarPrecio(codigo, precio, version);
        return ResponseEntity.ok(productosDto);
    }

    @GetMapping("/ordenes")
    public ResponseEntity<PaginaOrdenesDto> listarCompras(
            @PageableDefault(sort = {"fecha", "ordenesId"} , direction = Sort.Direction.DESC)
            Pageable pageable
    ) {
        PaginaOrdenesDto paginaOrdenesDto = microServicioService.listarOrdenes(pageable);
        return ResponseEntity.ok(paginaOrdenesDto);
    }

    @GetMapping("/ordenes/sucursal/{nombre}")
    public ResponseEntity< List<OrdenesDto>> obtenerSucursal(@PathVariable String nombre){
        List<OrdenesDto> resultado = microServicioService.obtenerSucursal(nombre);
        return ResponseEntity.ok(resultado);
    }

    @DeleteMapping("/ordenes/{id}")
    public ResponseEntity<Void> eliminarOrden (@PathVariable Long id){
        microServicioService.eliminarOrden(id);
        return ResponseEntity.noContent().build();

    }

}
