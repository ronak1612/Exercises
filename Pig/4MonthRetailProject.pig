

bagNov= LOAD '/home/hduser/Downloads/D11' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

DESCRIBE bagNov;
--bagNov: {DateTimes: chararray,CustomerID: int,Age: chararray,ResidenceArea: chararray,ProductCategory: int,ProductID: double,Qty: int,TotalCost: double,TotalSales: double}

--DUMP bagNov;

--(A1)Find out the customer I.D for the customer and the date of transaction who has spent the maximum amount in a month.


filteredData= FOREACH bagNov GENERATE $0,$1,$8;

--dump filteredData;

allgrouped=  GROUP filteredData ALL;

--DUMP allgrouped;


MAXSPENDED= FOREACH allgrouped GENERATE MAX(filteredData.TotalSales) AS MAXSPENT;

DUMP MAXSPENDED;
--(62688.0)


result= FILTER filteredData BY TotalSales == MAXSPENDED.MAXSPENT;

DUMP result;
--(2000-11-28 00:00:00,2119083,62688.0)


