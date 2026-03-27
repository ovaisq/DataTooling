# Agent Instructions for DataTooling Repository

## Build / Lint / Test Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Lint with ruff (recommended)
ruff check .
ruff check --fix .

# Type checking (optional)
mypy .

# Run ETL scripts
python3 s3_json_to_psql_etl.py --adt-feed-name "acme" --s3-bucket-prefix "bucket/prefix/*.json"
python3 s3_redis_json_to_psql_etl.py --adt-feed-name "redis_$(date '+%s')"

# Generate sample data
python3 create_sample_data.py
```

**Note:** This project does not use pytest. Testing is done via manual execution with sample data.

## Code Style Guidelines

### Imports
- Group imports: stdlib → third-party → local modules
- Use absolute imports for local modules (e.g., `from utils import read_config`)
- Place `from __future__` imports first if needed

### Formatting
- Follow PEP 8 conventions
- Use 4 spaces for indentation
- Max line length: 88-100 characters (Black compatible)
- Use double quotes for docstrings, single quotes acceptable for strings

### Naming Conventions
- **Functions:** `snake_case` (e.g., `get_spark_session`, `process_hl7_segment`)
- **Variables:** `snake_case` (e.g., `data_frame`, `field_name`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `IGNORE_FIELDS`, `HL7_SEGMENTS`)
- **Modules:** `snake_case` with descriptive names
- **Classes:** `PascalCase` (if any)

### Type Hints
- Optional but encouraged for new code
- Document function parameters in docstrings

### Error Handling
- Catch specific exceptions, not bare `except:`
- Use `logging` module for errors, not print statements
- Exit with non-zero code on critical failures: `sys.exit(-1)`
- Pattern:
  ```python
  try:
      result = some_operation()
  except SpecificException as e:
      logging.error("Operation failed: %s", e)
      sys.exit(-1)
  ```

### Logging
- Use `logging` module with INFO level for progress
- Format: `logging.info("**** Action Description **")`
- Include contextual data in log messages

### Documentation
- Use docstrings for all functions and modules
- Follow Google-style docstrings:
  ```python
  def function_name(arg):
      """Brief description.
      
      Args:
          arg: Description
          
      Returns:
          Description
      """
  ```

### Spark/PySpark Patterns
- Predefine schemas for DataFrame reads (performance critical)
- Use `.lower()` for column names consistently
- Truncate column names > 63 bytes for PostgreSQL compatibility
- Filter data before writing to reduce memory usage

### Configuration
- Use INI-style config files via `configparser`
- Store secrets in `etl.config` (not in code)
- Reference: `etl.config.template` for structure

### Database Operations
- Use JDBC for PostgreSQL writes from Spark
- Batch size: 2000 (configured in `df_to_jdbc`)
- Table naming: `v4_<feed_name>` (replace hyphens with underscores)

## Project Structure

- **ETL Scripts:** `s3_json_to_psql_etl.py`, `s3_redis_json_to_psql_etl.py`
- **Utilities:** `utils.py` (config reading, DB/Spark connections)
- **Data Generation:** `create_sample_data.py`
- **Configuration:** `etl.config`, `field_map.txt`, `hl7_field_names_to_ignore.txt`
- **Dependencies:** `requirements.txt`

## Environment Requirements

- Apache Spark 3.4.1+
- PostgreSQL with JDBC driver
- AWS S3 access (for S3 ETL)
- RedisJSON (for Redis ETL)
- Python 3.x

## Important Notes

- Scripts filter for states: CA, OR, WA, ID, UT only
- Spark JARs must be at `/var/tmp/sparkjars/`
- Mode "append" used for JDBC writes (duplicate handling manual)
