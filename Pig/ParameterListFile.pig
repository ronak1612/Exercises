

--ParameterListFile.pig

bagParamDemo= LOAD '$inputFilePath' USING PigStorage(',') AS (BookID:INT, Price:INT, AuthorID:INT);

DESCRIBE bagParamDemo;

DUMP bagParamDemo;

STORE bagParamDemo INTO '$outputFilePath';



