package practice.modelo;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@Entity
@Table(name = "ordenes")
public class Ordenes {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long ordenesId;

    @ManyToOne
    @JoinColumn(name = "sucursal_Id")
    private Sucursal sucursal;

    @Column(name = "fecha")
    private LocalDate fecha;

    @Column(name = "total", nullable = false)
    private BigDecimal total;

   @OneToMany(mappedBy = "ordenes", cascade = CascadeType.REMOVE, orphanRemoval = true)
   @BatchSize(size = 50)
    List<Productos> productos = new ArrayList<>();





}
