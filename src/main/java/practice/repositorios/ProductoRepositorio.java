package practice.repositorios;

import practice.modelo.Productos;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ProductoRepositorio extends JpaRepository<Productos, Long> {

    public Optional <Productos> findByCodigo(String codigo);
}
