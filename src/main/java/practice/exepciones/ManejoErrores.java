package practice.exepciones;

import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

@Slf4j
@RestControllerAdvice
public class ManejoErrores {

    // Atiende cualquier EntityNotFoundException lanzada desde el service
    @ExceptionHandler(EntityNotFoundException.class)
    public ResponseEntity<String> manejoNoEncontrado(EntityNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<String> manejoRecursoNoEncontrado(NoResourceFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Recurso no encontrado");
    }


    // Respaldo: cualquier otra excepción inesperada
    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> manejoGenerico(Exception ex) {
        log.error("Error inesperado", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error interno del servidor");
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    public ResponseEntity<String> duplicadoVersion(ObjectOptimisticLockingFailureException exception){
        log.warn("Datos desactualizados", exception);
        return ResponseEntity.status(HttpStatus.CONFLICT).body("El producto fue modificado por otro usuario. Por favor, recarga la página.");
    }

    @ExceptionHandler(ReglaNegocioException.class)
    public ResponseEntity<String> reglaNegocio(ReglaNegocioException exception){
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(exception.getMessage());

    }
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<String> dataIntegrityViolation(DataIntegrityViolationException exception){
        log.warn(" Violacion De Integridad ", exception);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(" EL Codigo del producto ya se encuentra en base de datos " );
    }
    

}