# Data Gen DataStores

### install mc
```shell
brew install minio/stable/mc
mc --help
```

### connect to minio
```shell
mc alias set do-nyc1-orn-polaris-dev http://138.197.224.4 data-lake 12620ee6-2162-11ee-be56-0242ac120002
mc ls do-nyc1-orn-polaris-dev/landing
```

### write data to [interface]
```shell
python cli.py all parquet
python cli.py all json

python cli.py mssql json
python cli.py postgres json
python cli.py mongodb json
python cli.py redis json


python cli.py mssql parquet
python cli.py postgres parquet
python cli.py mongodb parquet
python cli.py redis parquet
```