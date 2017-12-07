REGISTER '/home/acadgild/airline_usecase/piggybank.jar';
 
A = load '/home/acadgild/airline_usecase/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
B = FOREACH A GENERATE (chararray)$17 as origin, (chararray)$18 as dest, (int)$24 as diversion;
 
C = FILTER B BY (origin is not null) AND (dest is not null) AND (diversion == 1);
 
D = GROUP C by (origin,dest);
 
E = FOREACH D generate group, COUNT(C.diversion);
 
F = ORDER E BY $1 DESC;
 
Result = limit F 10;
 
dump Result;

/*
In Line 1: We are registering piggybank jar in order to use CSVExcelStorage class.

In relation A, we are loading the dataset using CSVExcelStorage because of its effective technique to handle double quotes and headers.

In relation B, we are generating the columns which are required for processing and explicitly type-casting each of them.

In relation C, we are filtering the data based on “not null” and diversion =1. This will remove the null records, if any, and give the data corresponding to the diversion taken.

In relation D, we are grouping the data based on origin and destination.
Relation D finds the count of diversion taken per unique origin and destination.

Relations F and Result orders the result and produces top 10 results.

*/
