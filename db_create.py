

account_400="""CREATE TABLE [research].[dbo].[account_400](
        [ID] bigint IDENTITY(1,1) NOT NULL,
        [IsCorporateAccount] bit NULL,
        [OrganizationID] bigint NULL,
        [CompanyID] bigint NULL,
        [Name] varchar(50) NULL,
        [webName] varchar(200) NULL,
        [FetchDate] datetime NULL,
        [Year]  bigint NULL,
        [Type] varchar(100) NULL,
        [Description] varchar(100) NULL,
        [Value] float NULL,
        [DescrID] float NULL,
        [Source] varchar(100) NULL,
        [Currency] varchar(100) NULL


        )"""


OID_400="""CREATE TABLE [research].[dbo].[OID_400](
        [ID] bigint IDENTITY(1,1) NOT NULL,
		[OwnerName] varchar (200) NULL,
		[CompName] varchar (200) NULL,
        [OrganizationID] bigint NULL,
		[DirectlyOwned] int NULL, 
		[owned_by] varchar (3000) NULL, 
		[owning] varchar (1000) NULL, 
		[desc] varchar (1000) NULL, 
		[hash] CHAR(64) NULL
        )"""


ownership="""CREATE TABLE [research].[dbo].[ownership](
        [ID] bigint IDENTITY(1,1) NOT NULL,
		
        [OrganizationID] bigint NULL,
		[OwnerOID] bigint NULL,
		[perc] float NULL,
		[level] int NULL, 
		[hash] CHAR(64) NULL
        )"""
