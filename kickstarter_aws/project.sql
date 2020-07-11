create database kickstarter;



CREATE EXTERNAL TABLE kickstarter.ft_project_analysis (
  `project_id` string, 
  `creator` string, 
  `state` string, 
  `country` string, 
  `title` string, 
  `description` string, 
  `category` string, 
  `location` string, 
  `names` array<string>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://unimib-dwh-2020/projects_dataset.out/'