bagBook= LOAD '/home/hduser/Downloads/bookdata' USING PigStorage(',') AS ( BookID: INT, Price: INT, AuthorID: INT);

bagAuthor= LOAD '/home/hduser/Downloads/authordata' USING PigStorage(',') AS ( AuthorID: INT, AuthorName);


bagJoin= JOIN bagBook by AuthorID, bagAuthor by AuthorID;

bagFinal= FOREACH bagJoin GENERATE $0, $1, $2, $4;

priceFilter1= FILTER bagFinal by Price>200;

NameFilter1= Filter priceFilter1 by INDEXOF(AuthorName,'J',0)==0;

dump NameFilter1;
