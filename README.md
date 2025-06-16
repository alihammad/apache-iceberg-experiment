# Apache Iceberg with Spark and Trino on Docker

This project provides a local development environment for experimenting with Apache Iceberg. It uses Apache Spark for data processing, Trino for SQL querying, and MinIO as an S3-compatible object storage solution, all running in Docker containers.

## Project Structure

```plaintext
apache-iceberg-experiment/
├── docker-compose.yml          # Docker services configuration
├── notebooks/                  # Jupyter notebooks directory
│   └── example.ipynb          # Example notebook with PySpark code
├── trino/                     # Trino configuration directory
│   ├── catalog/               # Trino catalog configurations
│   │   ├── iceberg.properties # Iceberg connector configuration
│   │   └── minio.properties   # MinIO connector configuration
│   └── etc/                   # Trino server configurations
│       ├── config.properties  # Main Trino configuration
│       ├── node.properties    # Node-specific configuration
│       └── jvm.config        # JVM settings for Trino
└── README.md                  # Project documentation

```

## Prerequisites

*   Docker Desktop (or Docker Engine and Docker Compose)
*   Git (for cloning, optional if you create files manually)

## Setup

1.  **Clone the Repository (Optional)**
    If you have this project in a Git repository:
    ```bash
    git clone apache-iceberg-experiment.git
    cd apache-iceberg-experiment
    ```

2.  **Create Directory Structure**
    If you are setting up manually, create the necessary directories:

    *   **Linux/macOS:**
        ```bash
        mkdir -p notebooks trino/catalog trino/etc
        ```
    *   **Windows PowerShell:**
        ```powershell
        New-Item -ItemType Directory -Force -Path "./notebooks"
        New-Item -ItemType Directory -Force -Path "./trino/catalog"
        New-Item -ItemType Directory -Force -Path "./trino/etc"
        ```

3.  **Configure Trino**

    Create the following configuration files:

    *   **`trino/etc/config.properties`** (Trino coordinator configuration):
        ```properties
        coordinator=true
        node-scheduler.include-coordinator=true
        http-server.http.port=8080
        query.max-memory=4GB
        query.max-memory-per-node=2GB
        discovery-server.enabled=true
        discovery.uri=http://localhost:8080
        ```

    *   **`trino/etc/jvm.config`** (Trino JVM options):
        ```
        -server
        -Xmx4G
        -XX:+UseG1GC
        -XX:G1HeapRegionSize=32M
        -XX:+UseGCOverheadLimit
        -XX:+ExplicitGCInvokesConcurrent
        -XX:+HeapDumpOnOutOfMemoryError
        -XX:+ExitOnOutOfMemoryError
        -Djdk.attach.allowAttachSelf=true
        -XX:-UseBiasedLocking
        ```
        *(Note: `TRINO_HEAP_MAX` and `TRINO_HEAP_MIN` in `docker-compose.yml` will override `-Xmx` if set, but it's good practice to have a `jvm.config`)*

    *   **`trino/catalog/iceberg.properties`** (Iceberg catalog for Trino):
        ```properties
        connector.name=iceberg
        iceberg.catalog.type=hadoop
        hive.metastore.uri=thrift://localhost:9083 # Or your chosen metastore if not using Hadoop catalog's default
        # For S3/MinIO access if not configured globally in Trino
        fs.hadoop.enabled=true
        s3.endpoint=http://minio:9000
        s3.aws-access-key=minioadmin
        s3.aws-secret-key=minioadmin
        s3.path-style-access=true
        # iceberg.hadoop.fs.s3a.endpoint=http://minio:9000
        # iceberg.hadoop.fs.s3a.access-key=minioadmin
        # iceberg.hadoop.fs.s3a.secret-key=minioadmin
        # iceberg.hadoop.fs.s3a.path-style-access=true
        ```
        *(Note: For a simple Hadoop catalog pointing to S3, you might not need a separate Hive Metastore if Spark writes the metadata directly to the S3 location. The `hive.metastore.uri` is typically for when you use a Hive Metastore service. If Spark is creating tables with a Hadoop catalog directly on S3, Trino needs to be configured to find those tables. The S3 properties are crucial here.)*

    *   **`trino/catalog/minio.properties`** (Optional: if you want a separate catalog for MinIO for non-Iceberg data, or to simplify S3 config for Iceberg if not using Hadoop catalog directly):
        ```properties
        connector.name=hive
        hive.metastore=file
        hive.metastore.catalog.dir=s3a://trino-hive-data/
        s3.endpoint=http://minio:9000
        s3.aws-access-key=minioadmin
        s3.aws-secret-key=minioadmin
        s3.path-style-access=true
        ```

4.  **Start Services**
    Navigate to the root directory of the project (where `docker-compose.yml` is located) and run:
    ```bash
    docker-compose up -d
    ```

## Access Points

*   **Jupyter Notebook (Spark):** http://localhost:8888
    *   You might need a token to log in the first time. Check the Docker logs for the Spark container: `docker logs spark`
*   **Trino UI:** http://localhost:8080
    *   Username: `trino` (or any username, as authentication is not configured by default)
    *   No password required by default.
*   **MinIO Console:** http://localhost:9001
    *   Username: `minioadmin`
    *   Password: `minioadmin`
*   **Spark UI:** http://localhost:4040 (Available when a Spark application is running)

## Usage Examples

### 1. Create an Iceberg Table using PySpark

Create a new notebook in Jupyter (`notebooks/example.ipynb`) and run the following:

```python
from pyspark.sql import SparkSession

# Define Iceberg version for clarity
iceberg_version = "1.4.2"
hadoop_aws_version = "3.3.2" # Matches Spark's Hadoop
aws_sdk_version = "1.11.1026"

spark = SparkSession.builder \
    .appName("Iceberg with MinIO Example") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Create a bucket in MinIO first (e.g., 'iceberg-warehouse') if it doesn't exist.
# You can do this via the MinIO console (http://localhost:9001)

# Create a database (namespace)
spark.sql("CREATE DATABASE IF NOT EXISTS local.default")

# Create a simple Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS local.default.customers (
    id BIGINT,
    name STRING,
    city STRING
)
USING iceberg
PARTITIONED BY (city)
""")

# Insert some data
data = [(1, "Alice", "New York"), (2, "Bob", "Chicago"), (3, "Carol", "New York")]
columns = ["id", "name", "city"]
df = spark.createDataFrame(data, columns)
df.writeTo("local.default.customers").append()

# Read the data
print("Reading from Iceberg table:")
spark.read.table("local.default.customers").show()

spark.stop()
Use code with care. Learn more
2. Query the Table using Trino
Open the Trino UI (http://localhost:8080), select the iceberg catalog (or the name you used in iceberg.properties), and run queries:

sql
SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.default;

SELECT * FROM iceberg.default.customers;

SELECT city, count(*)
FROM iceberg.default.customers
GROUP BY city;
