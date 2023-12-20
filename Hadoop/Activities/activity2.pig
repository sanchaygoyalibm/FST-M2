-- Load data from the file
inputFile = LOAD 'hdfs:///user/sanchay/input.txt' AS (line:chararray);
-- Tokenize the lines
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Group words by word [MAP]
grpd = GROUP words BY word;
-- Count the number of words [REDUCE]
totalCount = FOREACH grpd GENERATE $0 as word, COUNT($1) as word_count;
-- Store the output in HDFS
STORE totalCount INTO 'hdfs:///user/sanchay/PigOutput';
