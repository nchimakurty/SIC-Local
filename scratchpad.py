Project Struct

oracle-ingestion-dab/
├── bundle.yaml
├── workflows/
│   └── oracle_ingestion_workflow.dlt.py
├── configs/
│   └── dev.yaml
├── notebooks/
│   └── oracle_connector.py
└── README.md



Config File

bundle:
  name: oracle-ingestion
  version: 1.0.0

resources:
  pipelines:
    oracle-ingestion-pipeline:
      target: dev
      libraries:
        - notebook: ./workflows/oracle_ingestion_workflow.dlt.py
      configuration:
        spark.sql.shuffle.partitions: "4"
        data_source: "oracle"
        jdbc_url: "{{secrets.oracle.jdbc_url}}"
        user: "{{secrets.oracle.user}}"
        password: "{{secrets.oracle.password}}"

environments:
  dev:
    workspace:
      host: https://<your-databricks-instance>
    mode: development
    default: true
    permissions:
      - level: CAN_MANAGE
        group_name: users
    
