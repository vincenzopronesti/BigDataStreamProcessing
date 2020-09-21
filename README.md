# BigDataStreamProcessing
Repository for a big data project on strem processing.

This is a DSP project that uses Apache Flink to answer the following queries:
1. Compute the average school bus delay by neighborhood in the last 24 hours, 7 days, and 1 month.
2. Rank the three most common causes of delay in two-hours range 5:00-11:59 and 12:00-19:00.

#### Code execution
- From the `docker/client-image` directory build the client Docker image executing `sh run.sh`.
- From the `flinkMetrics` directory execute `sh run.sh`.
- From the `setup` directory run the following commands:
	- `docker-compose up -d`
	- `docker-compose exec flink_client /bin/bash /script/runQ1.sh`
	- `docker-compose exec flink_client /bin/bash /script/runQ2.sh`
	- `docker-compose down -v`

The output CSV files will be added in the `setup/script` directory.
