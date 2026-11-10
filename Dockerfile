FROM maven:3.10.0-eclipse-temurin-17-noble AS build
WORKDIR /app

# Capa de dependencias: solo se invalida si cambia el pom.xml
COPY pom.xml .
RUN mvn --batch-mode --no-transfer-progress dependency:go-offline

# Código fuente y empaquetado. Los tests ya se ejecutan en el CI (job Build & Verify).
COPY src ./src
RUN mvn --batch-mode --no-transfer-progress package -DskipTests

# ---- Etapa 2: runtime ----
# Solo JRE, glibc, versión exacta. Tag verificado en el catálogo oficial.
FROM eclipse-temurin:17.0.21_9-jre-noble

# Heap proporcional al límite de memoria del contenedor (sobrescribible desde el manifiesto)
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"

# UID/GID numéricos: Kubernetes solo puede verificar runAsNonRoot con IDs numéricos
RUN groupadd --system --gid 10001 appgroup \
 && useradd --system --uid 10001 --gid 10001 --no-create-home appuser

WORKDIR /app

# El .jar.original no encaja con *.jar: solo se copia el ejecutable
COPY --chown=10001:10001 --from=build /app/target/*.jar app.jar

EXPOSE 8080
USER 10001:10001

ENTRYPOINT ["java", "-jar", "app.jar"]