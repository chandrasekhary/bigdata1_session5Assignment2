REGISTER '/home/acadgild/airline_usecase/piggybank.jar';
 
A = load '/home/acadgild/airline_usecase/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
B1 = foreach A generate (int)$16 as dep_delay, (chararray)$17 as origin;
 
C1 = filter B1 by (dep_delay is not null) AND (origin is not null);
 
D1 = group C1 by origin;
 
E1 = foreach D1 generate group, AVG(C1.dep_delay);
 
Result = order E1 by $1 DESC;
 
Top_ten = limit Result 10;
 
Lookup = load '/home/acadgild/airline_usecase/airports.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
Lookup1 = foreach Lookup generate (chararray)$0 as origin, (chararray)$2 as city, (chararray)$4 as country;
 
Joined = join Lookup1 by origin, Top_ten by $0;
 
Final = foreach Joined generate $0,$1,$2,$4;
 
Final_Result = ORDER Final by $3 DESC;
 
dump Final_Result;

/*

Explanation of first 3 lines are the same as explained in the previous 2 problem statements.

In relation C1, we are removing the null values fields present if any.

In relation D1, we are grouping the data based on column “origin.”

In relation E1, we are finding average delay from each unique origin.

Relations named Result and Top_ten are ordering the results in descending order and printing the top ten values.

These steps are good enough to find the top ten origins with the highest average departure delay.
However, rather than generating just the code of origin, we will be following a few more steps to find some more details like country and city.

In the relation Lookup, we are loading another table to which we will look up and find the city as well as the country.

In the relation Lookup1, we are generating the destination, city, and country from the previous relation.

In the relation Joined, we are joining relation Top_ten and Lookup1 based on common a column, i.e., “origin.”

In the relation Final, we are generating required columns from the Joined table.
Finally, we are ordering and printing the results.
*/
