import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import practice.repositorios.OrdenRepositorio;
import practice.repositorios.ProductoRepositorio;
import practice.repositorios.SucursalRepositorio;
import practice.service.MicroServicioService;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MicroserviceTest {

    @Mock
    private OrdenRepositorio ordenRepositorio;
    @Mock
    private ProductoRepositorio productoRepositorio;
    @Mock
    private SucursalRepositorio sucursalRepositorio;



    @InjectMocks
    private MicroServicioService microServicioService;



    @Test
    public void obtenerOrden_cuandoNoExiste_lanzaExcepcion() {
        // Arrange: el repositorio falso dice que la orden 99 no existe
        when(ordenRepositorio.buscarConProducto(99L)).thenReturn(Optional.empty());

        // Act + Assert: llamar al service debe lanzar la excepción
        assertThrows(EntityNotFoundException.class,
                () -> microServicioService.obtenerOrden(99L));
    }







    }


