 -- Find out the top 5 most visited destinations

REGISTER '/home/acadgild/airline_usecase/piggybank.jar';
 
A = load '/home/acadgild/airline_usecase/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
B = foreach A generate (int)$1 as year, (int)$10 as flight_num, (chararray)$17 as origin,(chararray) $18 as dest;
 
C = filter B by dest is not null;
 
D = group C by dest;
 
E = foreach D generate group, COUNT(C.dest);
 
F = order E by $1 DESC;
 
Result = LIMIT F 5;
 
A1 = load '/home/acadgild/airline_usecase/airports.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
A2 = foreach A1 generate (chararray)$0 as dest, (chararray)$2 as city, (chararray)$4 as country;
 
joined_table = join Result by $0, A2 by dest;
 
dump joined_table;

/* documentation

In Line 1: We are registering the piggybank jar in order to use the CSVExcelStorage class.

In relation A, we are loading the dataset using CSVExcelStorage because of its effective technique to handle double quotes and headers.

In relation B, we are generating the columns that are required for processing and explicitly typecasting each of them.

In relation C, we are filtering the null values from the “dest” column.

In relation D, we are grouping relation C by “dest.”

In relation E, we are generating the grouped column and the count of each.

Relation F and Result is used to order and limit the result to top 5.
These are the steps to find the top 5 most visited destinations. However, adding few more steps in this process, we will be using another table to find the city name and country as well.

In relation A1, we are loading another table to which we will look-up and find the city as well as the country.

In relation A2, we are generating dest, city, and country from the previous relation.

In relation joined_table, we are joining Result and A2 based on a common column, i.e., “dest”

*/
