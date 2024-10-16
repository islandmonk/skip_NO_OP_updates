/*
The scenario:
I own an ETL process. That process involves ingesting data from disparate sources and making my database sync with the latest.

Let's say that I have a table called [catalog]. It contains my company's catalog of products.  The source of truth for this table sends me a CSV every four hours. I ingest that CSV and make sure my [catalog] table is sync'd with it. I use an update like this:

UPDATE mc 
SET
	  [field1] = tc.[field1]
	, [field2] = tc.[field2]
FROM [my].[catalog] as mc  
INNER JOIN [their].[catalog] as tc
	ON mc.id = tc.id
WHERE  mc.[field1] <> tc.[field1]
OR mc.[field2] <> tc.[field2]

This command will compare every match and filter out only those who will see meaningful change and updates those rows only. I don't want to touch any rows unnecessarily--the perenial task of ETL developers. 
No trivial updates will be executed (by trivial update, I mean an update to a row that changes *none* of the fields in that row).
The rows without changes remain as they were, unmolested. 
Unmolested is a good word to use here because updating a row while not impacting values comes at an unnecessary cost. In ram and at the disk, an update to a row physically deletes the row with the old values and inserts a new row with the new values. 
If we're not changing anything, then a trivial update is still causing churn in ram and on disk. Giving a table a way to ignore trivial updates automatically would be a big win for many scenarios. 

Now, let's just say that some ham-fisted developer makes an update through some client or in a procedure change that does something similar to a large table but doesn't include any measures to ignore rows not getting a 'real' update. 
Is there something that I can do to the table to make it act as if it knows to pass on trivial updates?




