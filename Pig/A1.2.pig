

bagNov= LOAD '/home/hduser/Downloads/D11' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagDec= LOAD '/home/hduser/Downloads/D12' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagJan= LOAD '/home/hduser/Downloads/D01' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagFeb= LOAD '/home/hduser/Downloads/D02' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);



allmonthData= UNION bagNov, bagDec, bagJan, bagFeb;

requiredData= FOREACH allmonthData GENERATE $0, $1, $8;

allmonthgrouped= GROUP requiredData ALL;

maxSpendedAmt= FOREACH allmonthgrouped GENERATE MAX(requiredData.TotalSales) AS MAXAMT;

DUMP maxSpendedAmt;
--(444000.0)

customerDetail= FILTER requiredData BY TotalSales == maxSpendedAmt.MAXAMT;

DUMP customerDetail;

--(2001-02-17 00:00:00,1622362,444000.0)


