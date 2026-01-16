# Lab 3: Cluster Spark & Lakehouse

Este proyecto despliega un cluster Spark completo (Master, Worker, Jupyter Driver) usando Docker y simula un flujo de datos Lakehouse (Bronce -> Plata -> Oro) con Delta Lake.

## Requisitos
- Docker y Docker Compose instalados.

## Instrucciones

1. **Levantar el Cluster**:
   \`\`\`bash
   docker-compose up --build -d
   \`\`\`

2. **Acceder a Jupyter Lab**:
   - Abre tu navegador en: [http://localhost:8888](http://localhost:8888)
   - No requiere token.

3. **Monitorizar Spark**:
   - Spark Master UI: [http://localhost:8080](http://localhost:8080)

4. **Ejecutar el Pipeline**:
   Dentro de Jupyter, navega a la carpeta \`notebooks\` y ejecuta los scripts en orden:
   1. \`01_ingest.py\`: Convierte el CSV a Delta Lake (Bronce).
   2. \`02_transform.py\`: Limpia los datos (Plata).
   3. \`03_analytics.py\`: Agrega datos por departamento (Oro).

## Estructura
- \`data/\`: Contiene el CSV de entrada y la carpeta \`lakehouse\` donde se guardarán los datos Delta.
- \`notebooks/\`: Scripts de PySpark.
