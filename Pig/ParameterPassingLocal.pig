
--Loading the path of the file using parameter inputFile.
--File is in local file system.

bagParamDemo= LOAD '$inputFile' USING PigStorage(',') AS (AuthorID: CHARARRAY, AuthorName: CHARARRAY);

DESCRIBE bagParamDemo;

DUMP bagParamDemo;



