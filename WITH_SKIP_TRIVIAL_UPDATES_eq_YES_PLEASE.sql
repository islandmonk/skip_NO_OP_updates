/*
	The proposed syntax:

	CREATE TABLE [dbo].[burp] (
		  [row_id]			int identity(1,1) primary key
		, [row_guid]		uniqueidentifier NOT NULL DEFAULT(newid())
		, [created]			datetime NOT NULL DEFAULT(getdate())
		, [message]			nvarchar(max) NOT NULL
		, [modified]		datetime NULL
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

	My ETL process starts with one guy creating a 50MB CSV of the data in his [burp] table.
	He does this every two hours. I receive the file, ingest it, and make sure that my 
	information is appropriately syncronized. 

	It is possible that the person who developed the information sync process understands
	that many times there can be very few inserts or actual state updates. Not everybody makes
	their sql statements with such considerations, though. The reason for this scenario
	is to illustrate that situations exist where data warehouse-ish / data mart-ish churn
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

	What follows will be the setup for turning the above table into a time machine. Let's keep in mind
	that in the above table, the only non meta-data data column is the [message] column. 

	To avoid trivial update churn on the above table, we'll have an INSTEAD OF trigger for the table.
	I create that trigger by using my row_hash_infrastructure.sql script


*/

-- DROP TABLE IF EXISTS [dbo].[burp]
-- DROP TABLE IF EXISTS [dbo].[burp_time_machine]

	CREATE TABLE [dbo].[burp] (
		  [row_id]			int identity(1,1) primary key
		, [row_guid]		uniqueidentifier NOT NULL DEFAULT(newid())
		, [message]			nvarchar(max) NOT NULL
		-- business rule:
		-- these three fields are managed by the table itself and are not setable
		-- by users. At least users and procedures should leave them alone.
		, [created]			datetime NOT NULL DEFAULT(getdate())
		, [modified]		datetime NOT NULL DEFAULT(getdate())
		, [deleted]			datetime NULL
	)


	
	-- Add row_hash column to table [dbo].[burp]
	-- this is the row_hash calculated column script created by my 
	-- row_hash_infrastructure script
	/*
	ALTER TABLE [dbo].[burp] ADD row_hash as HASHBYTES('sha2_512', CONCAT(
		  [row_guid]
		, [created]
		, [message]
		)
	) PERSISTED; 
	GO
	*/

	-- as a business rule, however, I won't ever allow a [row_guid] to be changed
	-- so I'll take it out of the hash calc.
	-- Similarly, the [created], [modified] and [deleted] columns are only meant to be updated by
	-- the table's own self-management. Take them out of the hash calc too
	-- For this exercise, this is the row_hash calculated column
	-- the function CONCAT requires at least two arguments, so I'll put created back
	-- in there.
	ALTER TABLE [dbo].[burp] ADD row_hash as HASHBYTES('sha2_512', CONCAT(
		  [created]
		, [message]
		)
	) PERSISTED; 
	GO


--------
-- INSTEAD OF UPDATE, INSERT, DELETE trigger for [dbo].[burp]
CREATE OR ALTER TRIGGER dbo_burp__instead_of_IUD ON [dbo].[burp]
INSTEAD OF UPDATE, INSERT, DELETE 
AS
	/*
	-- Doug@HillsBrother.com

	This is the definition of an INSTEAD OF trigger. Its initial purpose is to reduce churn on tables
	mostly for the sake of performance. There is nothing stopping you from altering this trigger to
	add other functionality. Keep in mind, also, that you can have AFTER UPDATE triggers on the same
	table as one with an INSTEAD OF UPDATE trigger. So that is still available to you even if you
	go with this approach.
	*/

	DECLARE @now datetime = getdate();

	UPDATE d
	SET	
		  [message] = i.[message]
		, [modified] = @now
	FROM [dbo].[burp] as d -- deleted
	INNER JOIN inserted as i 
		ON d.[row_id] = i.[row_id] 
	-- rows where there is no difference between inserted and deleted are ignored
	WHERE d.[row_hash] <> i.[row_hash] 

	-- insert all rows that are in the inserted psuedotable
	-- that don't have corresponding rows in the deleted psuedotable
	INSERT [dbo].[burp] (
 		  [row_guid]
		, [created]
		, [message]
		-- don't insert into [modified]
	)
	SELECT 
 		  [row_guid]
		, [created]
		, [message]
	FROM inserted as i
	WHERE NOT EXISTS (
		SELECT TOP 1 1
		FROM deleted as d
		WHERE d.[row_id] = i.[row_id] 	
	)

	-- Don't physically delete rows that are DELETEd. Instead, just mark
	-- them as deleted.
	-- mark as deleted all rows that are in the deleted psuedotable
	-- that don't have corresponding rows in the inserted psuedotable
	UPDATE t 
	SET [deleted] = @now
	FROM [dbo].[burp] as t
	INNER JOIN deleted as d
		ON d.[row_id] = t.[row_id] 
	WHERE NOT EXISTS (
		SELECT TOP 1 1
		FROM inserted as i
		WHERE d.[row_id] = i.[row_id] 	
	)
GO
--------------
	
/*
	Now our burp table is keeping an eye on itself so it can keep itself
	from churning unnecessarily. Now, extend that and turn it into a time
	machine. Here's one way to do that.

	-- create a history/auditing table. Same schema except the key includes [created]
*/


	CREATE TABLE [dbo].[burp_time_machine] (
		  [row_id]			int 
		, [row_guid]		uniqueidentifier NOT NULL 
		-- there isn't a modified column in this table
		, [message]			nvarchar(max) NOT NULL
		, [created]			datetime NOT NULL DEFAULT(getdate())
		, [modified]		datetime NULL
		, [deleted]			datetime NULL
		, PRIMARY KEY ([row_id], [created])
	)


GO
CREATE OR ALTER TRIGGER dbo_burp__audit ON [dbo].[burp]
FOR UPDATE, INSERT, DELETE 
AS
BEGIN
	DECLARE @now datetime = getdate();

	-- Every non-trivial update creates a new row version
	-- get them *all* in there
	INSERT [dbo].[burp_time_machine] (
		  [row_id]	
		, [row_guid]
		, [created]	
		, [message]	
	)
	SELECT 
		  [row_id]	
		, [row_guid]
		, COALESCE([modified], [created])	
		-- could use @now here, but I think it's important that these values match
		-- to the penny
		, [message]	
	FROM inserted

	-- some of the burp rows might have been deleted
	-- mark the last version of each deleted row here:

	UPDATE t
	SET [deleted] = @now -- mark this history/audit row as deleted
	FROM deleted as d
	INNER JOIN (
		SELECT 
			x.*
			, ROW_NUMBER() OVER (PARTITION BY [row_id] ORDER BY [created] DESC) as rn
		FROM [dbo].[burp_time_machine] as x
	) as t
		ON d.[row_id] = t.[row_id]
		AND t.rn = 1
	WHERE NOT EXISTS (
		SELECT TOP 1 1
		FROM inserted as i
		WHERE d.[row_id] = i.[row_id]
	)
END

/*
	No we have a history/audit table. Congratulations! You have just made a time machine!

	You can look at the state of burps as they were at any time you want! Here's a sample
	table-valued-function illustrating how to do that:
*/

GO
CREATE FUNCTION [dbo].[fnt_burp_state] (@theTime datetime)
RETURNS @burp TABLE (
	  [row_id]			int primary key
	, [row_guid]		uniqueidentifier NOT NULL  
	, [created]			datetime NOT NULL 
	, [message]			nvarchar(max) NOT NULL
	, [modified]		datetime NULL
)
AS
BEGIN
	DECLARE 
		@end_of_time datetime = '2050-04-01'

	-- this function will return a rowset that represents the state of
	-- the [dbo].[burp] table at the time @theTime parameter
	/*
	-- What did the [burp] table look like on my birthday?
		SELECT 
			  [row_id]		
			, [row_guid]	
			, [created]		
			, [message]		
			, [modified]	
		FROM [dbo].[fnt_burp_state] ('1972-05-31') as x
	*/

	INSERT @burp (
		  [row_id]		
		, [row_guid]	
		, [created]		
		, [message]		
		, [modified]	
	)
	SELECT 
		  [row_id]		
		, [row_guid]	
		, [created]		
		, [message]		
		, [deleted]	
	FROM (
		SELECT 
			  [row_id]		
			, [row_guid]	
			, [created]		
			, [message]		
			, [deleted]	
			, ROW_NUMBER() OVER (PARTITION BY [row_id] ORDER BY [created]) as rn
		FROM [dbo].[burp_time_machine]
		WHERE [created] <= @theTime
	) as x
	WHERE x.rn = 1
	AND ISNULL(x.[deleted], @end_of_time) >= @theTime

	RETURN;
END


