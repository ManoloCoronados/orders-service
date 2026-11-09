package practice.modelo;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Entity
@Setter
@Getter
@NoArgsConstructor
@Table(name = "productos")
public class Productos {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

     @Column(name = "codigo", nullable = false, unique = true)
     private String codigo;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "orden_id", nullable = false)
    private Ordenes ordenes;

     @Column(name = "descripcion")
     private String descripcion;


     @Column(name = "precio", nullable = false)
     private BigDecimal precio;


     @Version
    Long version;

}
