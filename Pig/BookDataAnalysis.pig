/*	BookData			AuthorData		
BookID, Price, AuthorID		AuthorID,	AuthorName         
100,	200,	10		10,		John
200,	150,	20		20,		Jerry
300,	200,	30		30,		Alan
400,	300,	40		40,		Cathy
500,	150,	50		50,		Mark
600,	300,	60		60,		Justin9

List of authors whose name starts with 'J' and having book price>200
solution:  BookID Price AuthorID  AuthorName
            600   300     60	   Justin9  
	    	

*/ 


--Step1: Load the BookData and AuthorData into separate bags.

bagBook= LOAD '/home/hduser/Downloads/bookdata' USING PigStorage(',') AS (BookID: INT, Price: INT, AuthorID: INT);

--DESCRIBE bagBook;
--SS12.1  bagBook: {BookID: int,Price: int,AuthorID: int}

--DUMP bagBook;
/*SS12.2   
(100,200,10)
(200,150,20)
(300,200,30)
(400,300,40)
(500,150,50)
(600,300,60)
*/

bagAuthors= LOAD ' /home/hduser/Downloads/authordata' USING PigStorage(',') AS (AuthorID: INT, AuthorName: CHARARRAY);

--DESCRIBE bagAuthors;
--12.3 bagAuthors: {AuthorID: int,AuthorName: chararray}

--DUMP bagAuthors;
/*12.4 (10,John)
(20,Jerry)
(30,Alan)
(40,Cathy)
(50,Mark)
(60,Justin9)*/



--Step2: Joining Two dataset bags

bagJoined= JOIN bagBook BY AuthorID, bagAuthors BY AuthorID;

--DESCRIBE bagJoined;
--12.5 bagJoined: {bagBook::BookID: int,bagBook::Price: int,bagBook::AuthorID: int,bagAuthors::AuthorID: int,bagAuthors::AuthorName: chararray}

--DUMP bagJoined;
/* 12.6
(100,200,10,10,John)
(200,150,20,20,Jerry)
(300,200,30,30,Alan)
(400,300,40,40,Cathy)
(500,150,50,50,Mark)
(600,300,60,60,Justin9)
 $0  $1  $2 $3  $4 */

--Step3: Making Final bag by removing repeating columns;

bagFinal= FOREACH bagJoined GENERATE $0,$1,$2,$4;
DUMP bagFinal;
/* 12.7
(100,200,10,John)
(200,150,20,Jerry)
(300,200,30,Alan)
(400,300,40,Cathy)
(500,150,50,Mark)
(600,300,60,Justin9)
$0  $1   $2   $3
*/ 

--Step4:  Filering the bagFinal for books having price>200

bagFilterPrice= FILTER bagFinal BY $1>200;
DUMP bagFilterPrice;

/*(400,300,40,Cathy)
(600,300,60,Justin9)*/

--Step5: Filering the bagFilterPrice for author name starting with 'J'

bagFilterName= FILTER bagFilterPrice BY SUBSTRING($3,0,1)=='J';
 --DUMP bagFilterName;
--12.9   (600,300,60,Justin9)

--Step6:  Storing result in to file.

STORE bagFilterName INTO '/home/hduser/Desktop/NIIT-Practicals/Module-Pig/BookDataAnalysisResult.txt' USING PigStorage('\t');
--12.10 

 





 




