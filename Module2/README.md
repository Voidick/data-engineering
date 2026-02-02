Question #1
Within the execution for Yellow Taxi data for the year 2020 and month 12, what is the uncompressed file size of yellow_tripdata_2020-12.csv?
Answer: B

To obtain the uncompressed file size, I added file size logging after the extract task in the Kestra workflow by introducing the following steps:

- id: size_csv
  type: io.kestra.plugin.core.storage.Size
  uri: "{{ outputs.extract.outputFiles[render(vars.file)] }}"

- id: log_size
  type: io.kestra.plugin.core.log.Log
  level: INFO
  message: "Extracted {{ render(vars.file) }} size: {{ outputs.size_csv.size }} bytes"


For questions #3-#5 with row count:
I downloaded everything using backfill method because it was more convenient than doing it manually. 

For question #2:
Rendered file = {{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv → green_tripdata_2020-04.csv for taxi=green and trigger.date in April 2020.

For question #6:
I used this: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List