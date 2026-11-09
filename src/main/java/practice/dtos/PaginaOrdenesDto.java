package practice.dtos;
import java.util.List;

public record PaginaOrdenesDto(
        List<OrdenesDto> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
}