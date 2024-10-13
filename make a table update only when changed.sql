/*

UPDATE ONLY WHEN CHANGED TABLE
SET FIELD = ''
ONLY WHEN CHANGED 


make a table update only when changed
use an acceptable persisted rowhash and
an INSTEAD OF UPDATE trigger

!!!!!

use this also to store row versions!!!!!

*/

SELECT  
	  [id]
	, [session_id]
	, [location_id]
	, [latitude]
	, [longitude]
	, [altitude]
	, [created]
	, (
		SELECT 
			  [session_id]
			, [location_id]
			, [latitude]
			, [longitude]
			, [altitude]
		FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
	) as row_hash
FROM [deficiencies_dev].[application].[session_ping]


ALTER TABLE [application].[session_ping] add modified datetime
UPDATE [application].[session_ping] SET modified = created  -- this also needed the same default value as created
ALTER TABLE [application].[session_ping] ALTER COLUMN modified datetime not null
ALTER TABLE [application].[session_ping] -- this would be really cool!!!! But it isn't allowed
ADD row_hash AS (
	SELECT 
		  [session_id]
		, [location_id]
		, [latitude]
		, [longitude]
		, [altitude]
	FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
) PERSISTED


ALTER TABLE [application].[session_ping] -- list of columns important enough to track
ADD row_hash AS HASHBYTES(
	  'SHA2_512'
	, CONCAT(
		  [session_id]
		, [location_id]
		, [latitude]
		, [longitude]
		, [altitude]
	)
) PERSISTED


GO


CREATE TRIGGER triu_session_ping__instead_of_update
ON [application].[session_ping]
INSTEAD OF UPDATE
AS
	UPDATE t 
	SET
		-- EVERY column in the row_hash calc must be here!!
		  [session_id]		= s.[session_id]			
		, [location_id]		= s.[location_id]
		, [latitude]		= s.[latitude]
		, [longitude]		= s.[longitude]
		, [altitude]		= s.[altitude]
		-- plus anything else you want
		, [modified]		= getutcdate()
	FROM [application].[session_ping] as t
	INNER JOIN inserted as s
		ON t.id = s.id
	WHERE t.row_hash <> s.row_hash  -- this is the secret sauce right here!!!!!!!
GO


UPDATE [application].[session_ping]
SET latitude = latitude

select * 
from [application].[session_ping]
WHERE modified <> created

select * from [application].[session_ping]

UPDATE [application].[session_ping] 
SET location_id = 0 
WHERE id <= 10

update [application].[session_ping] SET location_id = ISNULL(location_id, -1111111) 

update [application].[session_ping] SET location_id += 1 

select count(*) 
from slab.location_checklist_control

-- biggest table here. Use that

CREATE TABLE [dbo].[big_list](
	[location_id] [int] NOT NULL,
	[control_id] [int] NOT NULL,
	[value] [nvarchar](1000) NULL,
	[text] [nvarchar](1000) NULL,
	[created] [datetime] NOT NULL default(getdate()),
	[modified] [datetime] NOT NULL default(getdate()),
 CONSTRAINT [PK_big_list_sdf] PRIMARY KEY CLUSTERED 
(
	[location_id] ASC,
	[control_id] ASC
)
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[big_list_rh](
	[location_id] [int] NOT NULL,
	[control_id] [int] NOT NULL,
	[value] [nvarchar](1000) NULL,
	[text] [nvarchar](1000) NULL,
	[created] [datetime] NOT NULL default(getdate()),
	[modified] [datetime] NOT NULL default(getdate()),
	 row_hash AS HASHBYTES('SHA2_512', CONCAT(
		  [location_id]
		, [control_id]
		, [value]
		, [text]
)) PERSISTED
 CONSTRAINT [PK_big_list_rh] PRIMARY KEY CLUSTERED 
(
	[location_id] ASC,
	[control_id] ASC
)
) ON [PRIMARY]




GO
CREATE OR ALTER TRIGGER triu_big_list__instead_of_update
ON [dbo].[big_list_rh]
INSTEAD OF UPDATE
AS
	DECLARE @dummy int;

	--UPDATE t 
	--SET
	--	-- EVERY column in the row_hash calc must be here!!
	--	  [location_id]		= s.[location_id]			
	--	, [control_id]		= s.[control_id]
	--	, [value]			= s.[value]
	--	, [text]			= s.[text]
	--	-- plus anything else you want
	--	, [modified]		= getutcdate()
	--FROM [dbo].[big_list_rh] as t
	--INNER JOIN inserted as s
	--	ON t.location_id = s.location_id
	--	AND t.control_id = s.control_id
	--WHERE t.row_hash <> s.row_hash  -- this is the secret sauce right here!!!!!!!
GO


SELECT * 
FROM [dbo].[big_list_rh]
WHERE location_id = 274720
AND control_id = 147

UPDATE [dbo].[big_list_rh]
SET [value] = '0'
WHERE location_id = 274720
AND control_id = 147

SELECT 



-- 0xAFCD86B2CA8F9D1471BDDBC4E0075A4E482D2FEDAFDFC88A9BB6D3E9315B42AB5090978D344EED83B828A360A06233FE425FC6889B8E287EDDD5C11908FAE8A0
-- 0x95B9663898FA89B74CD3EC28DD0091C83C515CB801BC217D0A3ED4A4332841402B5923E62CEE59EE0DCCDB8E1D34890E4153B2A4CBB31CA68AC5D906E16727C3


set statistics io on
set statistics time on

-- populate them

INSERT [dbo].[big_list] (
	  [location_id]
	, [control_id]
	, [value] 
	, [text] 
	, [created]
	, [modified] 
)
SELECT 
	  [location_id]
	, [control_id]
	, [value] 
	, [text] 
	, [created]
	, [modified] 
FROM slab.location_checklist_control


INSERT [dbo].[big_list_rh] (
	  [location_id]
	, [control_id]
	, [value] 
	, [text] 
	, [created]
	, [modified] 
)
SELECT 
	  [location_id]
	, [control_id]
	, [value] 
	, [text] 
	, [created]
	, [modified] 
FROM slab.location_checklist_control




select top 10000 * from [dbo].[big_list_rh]

update [dbo].[big_list] SET control_id = control_id

update [dbo].[big_list_rh] SET control_id = control_id


