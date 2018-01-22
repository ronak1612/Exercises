
--Loading the path of the file using parameter hdfsFilePath.
--File is in HDFS system.

bagParamDemo= LOAD '$hdfsFilePath' USING PigStorage(',') AS (AuthorID: CHARARRAY, AuthorName: CHARARRAY);

DESCRIBE bagParamDemo;

DUMP bagParamDemo;

