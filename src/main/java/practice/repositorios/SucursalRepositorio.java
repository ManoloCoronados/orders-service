package practice.repositorios;

import practice.modelo.Sucursal;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SucursalRepositorio extends JpaRepository<Sucursal, Long> {

    public Optional< Sucursal > findByNombre(String nombre);
}
