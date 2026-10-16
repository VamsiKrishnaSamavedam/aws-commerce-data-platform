Synthetic Source System
        │
        │ CSV
        ▼
Python Bronze Ingestion
        │
        ├── file validation
        ├── SHA-256 checksum
        ├── batch ID
        ├── idempotency
        ├── logging
        └── upload verification
        │
        ▼
Amazon S3
        │
        ▼
Bronze Layer