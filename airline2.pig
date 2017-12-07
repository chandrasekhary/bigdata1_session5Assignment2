REGISTER '/home/acadgild/airline_usecase/piggybank.jar';
 
A = load '/home/acadgild/airline_usecase/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
 
B = foreach A generate (int)$2 as month,(int)$10 as flight_num,(int)$22 as cancelled,(chararray)$23 as cancel_code;
 
C = filter B by cancelled == 1 AND cancel_code =='B';
 
D = group C by month;
 
E = foreach D generate group, COUNT(C.cancelled);
 
F= order E by $1 DESC;
 
Result = limit F 1;
 
dump Result;

/* documentation

In Line 1: We are registering piggybank jar in order to use the CSVExcelStorage class.

In relation A, we are loading the dataset using CSVExcelStorage because of its effective technique to handle double quotes and header.

In relation B, we are generating the columns which are required for processing and explicitly typecasting each of them.

In relation C, we are filtering the data based on cancellation and cancellation code, i.e.,  canceled = 1 means flight have been canceled and cancel_code = ‘B’ means the reason for cancellation is “weather.” So relation C will point to the data which consists of canceled flights due to bad weather.

In relation D, we are grouping the relation C based on every month.

In relation E, we are finding the count of canceled flights every month.
Relation F and Result is for ordering and finding the top month based on cancellation.

*/
