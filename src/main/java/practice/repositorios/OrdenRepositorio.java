package practice.repositorios;


import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import practice.modelo.Ordenes;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface OrdenRepositorio extends JpaRepository <Ordenes, Long> {

    @Query("select o from Ordenes o join fetch o.productos where o.ordenesId =:id")
    Optional<Ordenes> buscarConProducto(@Param("id") Long id);

}
