/*2. 	MediclaimData(Name, Department, ClaimAmt)
			amy	hr	8000
			jack	hr	7500
			joe	finance	9000
			daniel	admin	4750
			tim	TS	4750
			tim	TS	3500
			tim	TS	2750
	Calculate the average of ClaimAmt for each user name.*/

--Step1: Load the MediclaimData in to bag.

bagMedicalData= LOAD '/home/hduser/Downloads/medical' USING PigStorage('\t') AS (Name: CHARARRAY, Department: CHARARRAY, ClaimAmt: DOUBLE);

DESCRIBE bagMedicalData;
--12.2.1 bagMedicalData: {Name: chararray,Department: chararray,ClaimAmt: double}

DUMP bagMedicalData;
/*12.2.2
(amy,hr,8000.0)
(jack,hr,7500.0)
(joe,finance,9000.0)
(daniel,admin,4750.0)
(tim,TS,4750.0)
(tim,TS,3500.0)
(tim,TS,2750.0)*/

--Step2: GROUP by name so their all mediclaim amount will be together.

bagGroupName= GROUP bagMedicalData BY Name;

--DUMP bagGroupName;
/*12.2.3
(amy,{(amy,hr,8000.0)})
(joe,{(joe,finance,9000.0)})
(tim,{(tim,TS,2750.0),(tim,TS,3500.0),(tim,TS,4750.0)})
(jack,{(jack,hr,7500.0)})
(daniel,{(daniel,admin,4750.0)})
*/

--Step 3: Calculate the Average of ClaimAnt for each group.  

bagAvgAmt= FOREACH bagGroupName GENERATE group AS Name, AVG(bagMedicalData.ClaimAmt) AS AverageClaim;

--DUMP bagAvgAmt;

/* 12.2.4
(amy,8000.0)
(joe,9000.0)
(tim,3666.6666666666665)
(jack,7500.0)
(daniel,4750.0) */

--Here Average has more number of decimal's. So Rounding it to 2.


averageClaimForEachUser= FOREACH bagAvgAmt GENERATE $0, ROUND_TO($1,2) AS Average;

DUMP averageClaimForEachUser;
/* AVERAGE CLAIM FOR EACH USER
(amy,8000.0)
(joe,9000.0)
(tim,3666.67)
(jack,7500.0)
(daniel,4750.0)*/











