
CREATE TABLE sucursal (
                          id BIGSERIAL PRIMARY KEY,
                          nombre VARCHAR(255) NOT NULL,
                          CONSTRAINT uq_sucursal_nombre UNIQUE (nombre)
);

CREATE TABLE ordenes (
                         ordenes_id BIGSERIAL PRIMARY KEY,
                         fecha DATE,
                         total NUMERIC(38, 2) NOT NULL,
                         sucursal_id BIGINT,
                         CONSTRAINT fk_ordenes_sucursal FOREIGN KEY (sucursal_id) REFERENCES sucursal(id) ON DELETE RESTRICT
);


CREATE TABLE productos (
                           id BIGSERIAL PRIMARY KEY,
                           codigo VARCHAR(255) NOT NULL,
                           descripcion VARCHAR(255),
                           precio NUMERIC(38, 2) NOT NULL,
                           version BIGINT NOT NULL DEFAULT 0,
                           orden_id BIGINT NOT NULL,
                           CONSTRAINT uq_productos_codigo UNIQUE (codigo),
                           CONSTRAINT fk_productos_ordenes FOREIGN KEY (orden_id) REFERENCES ordenes(ordenes_id) ON DELETE CASCADE
);


CREATE INDEX idx_ordenes_sucursal ON ordenes(sucursal_id);
CREATE INDEX idx_productos_ordenes ON productos(orden_id);
