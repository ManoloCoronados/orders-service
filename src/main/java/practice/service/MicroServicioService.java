package practice.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import practice.dtos.OrdenesDto;
import practice.dtos.PaginaOrdenesDto;
import practice.dtos.ProductosDto;
import practice.events.OrderCreatedEvent;
import practice.exepciones.ReglaNegocioException;
import practice.modelo.Ordenes;
import practice.modelo.Productos;
import practice.modelo.Sucursal;
import practice.outbox.OutboxWriter;
import practice.repositorios.OrdenRepositorio;
import practice.repositorios.ProductoRepositorio;
import practice.repositorios.SucursalRepositorio;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class MicroServicioService {

    private final OrdenRepositorio ordenRepositorio;
    private final ProductoRepositorio productoRepositorio;
    private final SucursalRepositorio sucursalRepositorio;
    private final OutboxWriter outboxWriter;


    // POST
    @Transactional
    public Long guardarOrden(OrdenesDto ordenesDto) {
        long inicio = System.currentTimeMillis();
        log.info("Inicio guardarOrden");
        validarProducto(ordenesDto);
       /// validarCodigo(ordenesDto);

        Sucursal sucursal = sucursalRepositorio.findByNombre(ordenesDto.nombreSucursal())
                .orElseThrow(() -> new EntityNotFoundException(
                        " La sucursal " + ordenesDto.nombreSucursal() + " no existe "));


        Ordenes ordenes = new Ordenes();
        ordenes.setTotal(calcularTotal(ordenesDto.productosDtoList()));
        ordenes.setFecha(LocalDate.now());
        ordenes.setSucursal(sucursal);
        ordenRepositorio.save(ordenes);

        List<OrderCreatedEvent.Item> items = new ArrayList<>();
        for (ProductosDto productosDto : ordenesDto.productosDtoList()) {
            Productos productos = new Productos();
            productos.setOrdenes(ordenes);
            productos.setCodigo(productosDto.codigo());
            productos.setPrecio(productosDto.precio());
            productos.setDescripcion(productosDto.descripcion());
            productoRepositorio.save(productos);
        }
        for (ProductosDto productosDto: ordenesDto.productosDtoList()) {
            items.add(new OrderCreatedEvent.Item(
                    productosDto.codigo(), productosDto.descripcion(), productosDto.precio()));
        }
        outboxWriter.enqueueOrderCreated(new OrderCreatedEvent.Payload(
                ordenes.getOrdenesId(), sucursal.getNombre(), ordenes.getTotal(), items));

        long fin = System.currentTimeMillis();
        log.info("Fin guardarOrden: {} ms", fin - inicio);
        return ordenes.getOrdenesId();
    }

    // GET por id
    @Transactional(readOnly = true)
    public OrdenesDto obtenerOrden(Long id) {
        Ordenes ordenes = ordenRepositorio.buscarConProducto(id)
                .orElseThrow(() -> new EntityNotFoundException(" La orden " + id + " no existe "));

        List<ProductosDto> productosDtoList = new ArrayList<>();
        for (Productos productos : ordenes.getProductos()) {
            ProductosDto productosDto = new ProductosDto(
                    productos.getId(),
                    productos.getCodigo(),
                    productos.getDescripcion(),
                    productos.getPrecio(),
                    productos.getVersion()
            );
            productosDtoList.add(productosDto);
        }

        return new OrdenesDto(
                ordenes.getOrdenesId(),
                ordenes.getTotal(),
                ordenes.getSucursal().getNombre(),
                productosDtoList
        );
    }

    // PUT
    @Transactional
    public ProductosDto actualizarPrecio(String codigo, BigDecimal precio, Long version) {
        Productos productos = productoRepositorio.findByCodigo(codigo)
                .orElseThrow(() -> new EntityNotFoundException(" El producto " + codigo + " no existe"));
        if (!version.equals(productos.getVersion())) {
            throw new ObjectOptimisticLockingFailureException(" Datos desactualizados por favor recargar la pagina", null);       }
        productos.setPrecio(precio);
        productoRepositorio.saveAndFlush(productos);

        return new ProductosDto(
                productos.getId(),
                productos.getCodigo(),
                productos.getDescripcion(),
                productos.getPrecio(),
                productos.getVersion()
        );
    }

    // GET todas las órdenes (Ticket 1)
    @Transactional(readOnly = true)
    public PaginaOrdenesDto listarOrdenes(Pageable pageable) {
        Page<Ordenes> paginas = ordenRepositorio.findAll(pageable);
        List<OrdenesDto> ordenesDtoList = new ArrayList<>();

        for (Ordenes orden : paginas.getContent()) {
            List<ProductosDto> productosDtoList = new ArrayList<>();

            for (Productos productos : orden.getProductos()) {
                ProductosDto productosDto = new ProductosDto(
                        productos.getId(),
                        productos.getCodigo(),
                        productos.getDescripcion(),
                        productos.getPrecio(),
                        productos.getVersion()
                );
                productosDtoList.add(productosDto);
            }

            OrdenesDto ordenesDto = new OrdenesDto(
                    orden.getOrdenesId(),
                    orden.getTotal(),
                    orden.getSucursal().getNombre(),
                    productosDtoList
            );
            ordenesDtoList.add(ordenesDto);
        }

        PaginaOrdenesDto paginaOrdenesDto = new PaginaOrdenesDto(
                ordenesDtoList,
                paginas.getNumber(),
                paginas.getSize(),
                paginas.getTotalElements(),
                paginas.getTotalPages()

        );
        return paginaOrdenesDto;
    }

    // GET órdenes por sucursal (Ticket 2)
    @Transactional(readOnly = true)
    public List<OrdenesDto> obtenerSucursal(String nombre) {
        Sucursal sucursal = sucursalRepositorio.findByNombre(nombre)
                .orElseThrow(() -> new EntityNotFoundException(" La sucursal " + nombre + " no existe"));

        List<OrdenesDto> ordenesDtoList = new ArrayList<>();

        for (Ordenes ordenes : sucursal.getOrdenes()) {
            List<ProductosDto> productosDtoList = new ArrayList<>();

            for (Productos productos : ordenes.getProductos()) {
                ProductosDto productosDto = new ProductosDto(
                        productos.getId(),
                        productos.getCodigo(),
                        productos.getDescripcion(),
                        productos.getPrecio(),
                        productos.getVersion()
                );
                productosDtoList.add(productosDto);
            }

            OrdenesDto ordenesDto = new OrdenesDto(
                    ordenes.getOrdenesId(),
                    ordenes.getTotal(),
                    ordenes.getSucursal().getNombre(),
                    productosDtoList
            );
            ordenesDtoList.add(ordenesDto);
        }

        return ordenesDtoList;
    }

    // DELETE (Ticket 3)
    @Transactional
    public void eliminarOrden(Long id) {
        Ordenes ordenes = ordenRepositorio.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(" La orden " + id + " no existe"));

        ordenRepositorio.delete(ordenes);
    }


    private BigDecimal calcularTotal(List<ProductosDto> productoDto ){
        BigDecimal total = BigDecimal.ZERO;
        for (ProductosDto p : productoDto) {
            total = total.add(p.precio());

        }
        return total;
    }


    private void validarProducto(OrdenesDto ordenesDto){
        List<ProductosDto> productosDto = ordenesDto.productosDtoList();
        if ( productosDto == null || productosDto.isEmpty() ){
            throw new ReglaNegocioException(" La orden No tiene Productos" );
        }
        for (ProductosDto p : productosDto) {
            if ( p.precio()==null || p.precio().compareTo(BigDecimal.ZERO) <=0 ){
                throw new ReglaNegocioException(" Precio invalido para el Producto " + p.codigo());

            }
        }

    }

    private void validarCodigo(OrdenesDto ordenesDto){
        HashSet<String> codigos = new HashSet<>();

        for (ProductosDto p : ordenesDto.productosDtoList()) {

            if (p.codigo() == null || p.codigo().isBlank() ) {
                throw new ReglaNegocioException(" Codigo vacio para el Producto " + p.codigo());
            }
            if (!codigos.add(p.codigo())) {
                throw new ReglaNegocioException(" Codigo repetido en a Orden " + p.codigo());
            }
            if (productoRepositorio.findByCodigo(p.codigo()).isPresent()){
                throw new ReglaNegocioException(" El Producto " + p.codigo() + " ya existe");
            }

        }

    }


}