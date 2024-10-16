/*
	The proposed syntax:

	CREATE TABLE tbl_burp (
		  row_id int identity(1,1) primary key
		, row_guid uniqueidentifier NOT NULL DEFAULT(newid())
		, created datetime NOT NULL DEFAULT(getdate())
		, message nvarchar(max) NOT NULL
		, modified datetime NULL
	)
	WITH (SKIP_TRIVIAL_UPDATES = ON)


	I don't think that this scenario takes too much explanation. The internet is repleat
	with a few who are familiar with optimizing. We all have found ways to limit updates
	to a table with various comparisons. Each thinks their solution is novel and effective.

	I, thinking I'm special, have developed an approach that *is* novel and effective. The
	benefits seem pretty huge.

	The scenario is that I have a table as described above (without the change hint). In datapipeline
	/ data warehouse situations large swathes of this sort of data gets sync'd from one place 
	to another (usually through CSVs or fax). This method of data syncing is stupid and prevalent. 
	Poorman's replication.

	A fairly common way to sync a source with a target is with a MERGE statement where we 
	bump the source up against the target by their primary keys, update when matched, insert 
	when not. It shouldn't be a common way to sync these sorts of tables, but it is.

	I'm not alone in thinking this is silly and wasteful. I've seen a few approaches
	to minimize this and haven't really liked any. 

	I make tables behave this way already with the use of an 
	INSTEAD OF INSERT, UPDATE, DELETE TRIGGER
	This trigger fires in leiu of an actual operation of any kind. To enable MERGE operations, 
	the INSTEAD OF trigger needs to be on all operations. From here on out, we're
	only talking about updates.

	My ETL process starts with one guy creating a 50MB CSV of the data in his burp table.
	He does this every two hours. I receive the file, ingest it, and make sure that my 
	information is appropriately syncronized. 

	It is possible that the person who developed the information sync process understands
	that many times there can be very few inserts or actual state updates. Not everybody makes
	their sql statements with such considerations, though. The reason for this scenario
	is to illustrate that situations exist where data warehouse-ish / data mart-ish burn a
	lots of ram and ssd unnecessessarily while blocking unnecessarily.

	persist a rowhash

	before update, rowhash different? proceed : fuck_off

	What did we save when we fucked off?
	log pages in ram, data pages in ram, syncing ram pages with disk


	How'd I get to this? Really? You're interested? No you're not.

	Anyway, I've been working with data for a while now. I've been a lazy guy much longer
	than that. My ultimate career path drove me to dataware house sorts of activities. My
	first go with largeish (at the time) data was when I worked with Expedia's AdWords team.
	Since my ego craves validation, my sniffer always took me to the causes of performance
	pain. Blah blah blah, simple fix, beers all around.

	My data syncing techniques have evolved to this novel stage. "He thinks it's 'Novel'."
	A rowhash that we trust is persisted as a calculated column on the table. We trust a 
	hash that will 'never' give us false negatives! False positives are not really noticeable.

	in the INSTEAD OF trigger, the guilty table (deleted) is bumped up to the inserted psuedotable
	by their primary keys. Only continue with the update for that row where the rowhash columns 
	disagree.

	Having a table behave this way opens up interesting prospects with row_versioning and junk
	like that. If I can gaurantee that a row will only physically update if the update is
	non-trivial, then the AFTER UPDATE trigger means an awful lot more!!! This makes it so much
	easier to turn tables into time machines.

*/