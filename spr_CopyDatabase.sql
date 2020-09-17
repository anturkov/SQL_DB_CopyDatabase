USE [scs_AdminDB]
GO


-- =============================================
-- Author:      Antonio Turkovic (Data & AI CE at Microsoft)
-- Create date: 2020-09-16
-- Version:		1.00
-- Description: Copies an existing database to the local SQL instance
-- Parameters:
-- 		@dbName 			- Name of the database you want to copy
--		@SQLDumpDir 		- Directory to store the database backup (default = X:\SQL_Dump\Default\)
-- 		@backupSoftwareName - (optional) Name of the third party backup tool
--							- must be the exact name as in column "Software" in table msdb.dbo.backupmediaset
--							- SELECT software_name FROM msdb.dbo.backupmediaset
--		@backupAge 			- maximum age of a found full backup (default = 1)
-- 		@targetDBName 		- (optional) provide a specific name for the target database name
--					  		- if no name provided, the target name will be: <sourceDB>_<suffix>_<date><time>
--					  		- e.g.: TestDB_COPY_19001231235959
-- Returns: 	Specifc return code
--				0  - Successfull
--				10 - Source Database not found
--				15 - Target database already exists
--				17 - Database is running on Secondary replica
--				20 - Not enough free disk space on data disk
--				21 - Not enough free disk space on log disk
--				30 - Backup of database failed (no access to SQL Dump directory or not enough disk space)
--				40 - Could not read file configuration from backup file (FILELISTONLY)
--				41 - Database restore failed
-- Example:
--				EXEC dbo.spr_CopyDatabase @dbName = 'TestDB' -- Automatic 
--				EXEC dbo.spr_CopyDatabase @dbName = 'TestDB', @targetDBName = 'TestDB2' -- specific target database name
-- =============================================

CREATE OR ALTER PROCEDURE spr_CopyDatabase
	-- database to copy
	@dbName NVARCHAR(256),
	-- dump directory for backups
	@SQLDumpDir NVARCHAR(MAX) = N'X:\SQL_Dump\Default\',
	-- third party backup tool name (optional)
	@backupSoftwareName NVARCHAR(MAX) = N'Networker',
	-- max age of last full backup in days
	@backupAge INT = 1,
	-- copy DB name (optional)
	@targetDBName NVARCHAR(256) = NULL
AS
BEGIN
	
	SET NOCOUNT ON;

	--########################################
	-- VARIABLES

	-- Current Date Format
	DECLARE @thisDate NVARCHAR(MAX) = (SELECT FORMAT(GETDATE(), 'yyyyMMdd'))
	
	-- Current Time Format
	DECLARE @thisTime NVARCHAR(MAX) = (SELECT FORMAT(GETDATE(), 'HHmmss'))
	
	-- Command Var for Dynamic SQL
	DECLARE @cmd NVARCHAR(MAX) = ''

	-- Param Definition for Dyn SQL
	DECLARE @paramDefinition NVARCHAR(MAX) = ''

	-- Command Var for XPCMDShell
	DECLARE @xpCMD VARCHAR(8000) = ''

	-- Var for Messages
	DECLARE @msg NVARCHAR(4000) = ''

	-- Default Data File Location
	DECLARE @defaultData NVARCHAR(MAX) = (SELECT CONVERT(NVARCHAR(MAX), SERVERPROPERTY('InstanceDefaultDataPath')))

	-- Default Log File Location
	DECLARE @defaultLog NVARCHAR(MAX) = (SELECT CONVERT(NVARCHAR(MAX), SERVERPROPERTY('InstanceDefaultLogPath')))

	-- is HADR enabled
	DECLARE @isHADR INT = (SELECT CONVERT(INT, SERVERPROPERTY('IsHadrEnabled')))

	-- Define Clone method 
		-- CopyBackup = ThirdParty Backup Tool in use - perform manual copy-only backup
		-- NativeBackup = Built-in SQL Backup in use - restore from existing full backup if exists
	DECLARE @cloneMethod NVARCHAR(MAX) = 'CopyBackup'

	-- Clone Method recheck flag
	DECLARE @cloneMethodRecheck BIT = 0

	-- Source DB Data File Size
	DECLARE @sourceDataSize INT 

	-- Source DB Log File Size
	DECLARE @sourceLogSize INT

	-- Source DB Used Space MB
	DECLARE @sourceUsedSize INT

	-- Advanced Options Status
	DECLARE @chkAdvOptions SQL_VARIANT = (SELECT value FROM sys.configurations WHERE name = 'show advanced options')
	-- XPCMDShell Status
	DECLARE @chkCMDShell SQL_VARIANT = (SELECT value FROM sys.configurations WHERE name = 'xp_cmdshell')

	-- Table for Disk Free Spaces
	DECLARE @diskFreeSpace TABLE (
		DriveType NVARCHAR(512),
		Drive NVARCHAR(MAX),
		FreeSpaceMB BIGINT
	)

	-- Tmp Table for Disk FreeSpace XP_CMDShell results
	DECLARE @tmpDiskSpace TABLE (result NVARCHAR(MAX))

	-- Backup File Path
	DECLARE @backupFilename NVARCHAR(2048) = ''

	-- Target DB Suffix
	DECLARE @targetDBSuffix NVARCHAR(32) = 'COPY'

	-- DB File Counter
	DECLARE @dbFileCount INT = 1

	-- DB Backup File List only table
	DECLARE @fileList TABLE (
		LogicalName NVARCHAR(512),
		PhysicalName NVARCHAR(260),
		Type NVARCHAR(4),
		FileGroupName NVARCHAR(512),
		Size NUMERIC(20,0),
		MaxSize NUMERIC(20,0),
		FileId BIGINT,
		CreateLSN NUMERIC(25,0),
		DropLSN NUMERIC(25,0),
		UniqueID UNIQUEIDENTIFIER,
		ReadOnlyLSN NUMERIC(25,0),
		ReadWriteLSN NUMERIC(25,0),
		BackupSizeInBytes BIGINT,
		SourceBlockSize INT,
		FileGroupId INT,
		LogGroupGUID UNIQUEIDENTIFIER,
		DifferentialBaseLSN NUMERIC(25,0),
		DifferentialBaseGUID UNIQUEIDENTIFIER,
		IsReadOnly BIT,
		IsPresent BIT,
		TDEThumbprint VARBINARY(32),
		SnapshotURL NVARCHAR(36)
	)

	-- Return Code
	DECLARE @returnCode INT = 0
	/*
		Return Code Definition:
		0  - Successfull
		10 - Source Database not found
		15 - Target database already exists
		17 - Database is running on Secondary replica
		20 - Not enough free disk space on data disk
		21 - Not enough free disk space on log disk
		30 - Backup of database failed (no access to SQL Dump directory or not enough disk space)
		40 - Could not read file configuration from backup file (FILELISTONLY)
		41 - Database restore failed
	*/


	--#################################################
	-- VERIFICATION
	-- Check if last character of SQLDUmpDir is \ --> else Add 
	SET @SQLDumpDir = TRIM(@SQLDumpDir)
	IF(RIGHT(@SQLDumpDir, 1) != '\')
	BEGIN
		SET @SQLDumpDir = @SQLDumpDir + '\'
	END

	-- Check if last character in Default Data Path is \
	SET @defaultData = TRIM(@defaultData)
	IF(RIGHT(@defaultData, 1) != '\')
	BEGIN
		SET @defaultData = @defaultData + '\'
	END

	--Check if last character in Default Log Path is \
	SET @defaultLog = TRIM(@defaultLog)
	IF(RIGHT(@defaultLog, 1) != '\')
	BEGIN
		SET @defaultLog = @defaultLog + '\'
	END

	-- Check if source database exists
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Checking if database exists'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @dbName)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database "' + @dbName + '" not found - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT
		SET @returnCode = 10
		RETURN @returnCode;
	END

	-- Check if target DB already exists
	IF((@targetDBName IS NOT NULL) AND (TRIM(@targetDBName) != ''))
	BEGIN
		IF EXISTS(
			SELECT 1 FROM master.sys.databases WHERE name = @targetDBName
		)
		BEGIN
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Target database "' + @targetDBName + '" already exists - terminating script'
			RAISERROR(@msg, 10, 1) WITH NOWAIT
			SET @returnCode = 15
			RETURN @returnCode;
		END
	END
	ELSE
	BEGIN
		SET @targetDBName = @dbName + '_' + @targetDBSuffix + '_' + @thisDate + @thisTime
	END
	
	-- is Primary
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Checking HADR configuration'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	IF(((SELECT sys.fn_hadr_is_primary_replica(@dbName)) != 1) AND (@isHADR = 1))
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database is running on a Secondary replica - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT
		SET @returnCode = 17
		RETURN @returnCode;
	END	

	-- Get Source DB Data File Size
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Collecting database file sizes'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @sourceDataSize = (
		SELECT SUM(size) * 8 /1024 AS SizeMB
		FROM master.sys.master_files
		WHERE database_id = DB_ID(@dbName)
		AND type = 0
	)
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Database total data file size (MB): ' + CONVERT(NVARCHAR(MAX), @sourceDataSize)
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	--Get Source DB Log File Size
	SET @sourceLogSize = (
		SELECT SUM(size) * 8 /1024 AS SizeMB
		FROM master.sys.master_files
		WHERE database_id = DB_ID(@dbName)
		AND type = 1
	)
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Database total log file size (MB): ' + CONVERT(NVARCHAR(MAX), @sourceLogSize)
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	-- Get used data space
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Collecting database content size'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @cmd = '
		USE ['+ @dbName +'];
		SELECT @resultOut = SUM (CAST(FILEPROPERTY(name, ''SpaceUsed'') AS INT)/128)
		FROM sys.database_files
	';
	SET @paramDefinition = N'@resultOut INT OUTPUT'
	EXEC sp_executesql @cmd, @paramDefinition, @resultOut = @sourceUsedSize OUTPUT;
	
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Database total content size (MB): ' + CONVERT(NVARCHAR(MAX), @sourceUsedSize)
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	
	-- Check VLF count
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Collecting database VLF count'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	IF((SELECT COUNT(1) FROM sys.dm_db_log_info(DB_ID(@dbName))) > 1000)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | More than 1000 VLFs in source database - backup/restore process might take more time'
		RAISERROR(@msg, 10, 1) WITH NOWAIT
	END

	-- Check Free Space on Disk
	-- Enable Advanced Options (if not already enabled)
	IF(@chkAdvOptions = 0)
	BEGIN
		EXEC sp_configure 'show advanced options', 1;
		RECONFIGURE;
	END

	-- enable xp_cmdshell (if not already enabled)
	IF(@chkCMDShell = 0)
	BEGIN
		EXEC sp_configure 'xp_cmdshell', 1;
		RECONFIGURE;
	END

	-- Data Disk
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Collecting data disk free space'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @xpCMD = '
		powershell.exe -c "Get-Volume -FilePath ''' + CONVERT(VARCHAR(MAX), @defaultData) + ''' | Select -ExpandProperty SizeRemaining"
	';
	INSERT INTO @tmpDiskSpace (result)
	EXEC xp_cmdshell @xpCMD

	-- Add to table Data FreeSpace
	INSERT INTO @diskFreeSpace (DriveType, Drive, FreeSpaceMB)
	VALUES (
			'Data',
			@defaultData,
			(SELECT TOP (1) CONVERT(BIGINT, result) /1024/1024 FROM @tmpDiskSpace)
	)

	DELETE FROM @tmpDiskSpace
	
	-- Log DIsk
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Collecting log disk free space'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @xpCMD = '
		powershell.exe -c "Get-Volume -FilePath ''' + CONVERT(VARCHAR(MAX), @defaultLog) + ''' | Select -ExpandProperty SizeRemaining"
	';

	INSERT INTO @tmpDiskSpace (result)
	EXEC xp_cmdshell @xpCMD

	-- Add to Table log FreeSpace
	
	INSERT INTO @diskFreeSpace (DriveType, Drive, FreeSpaceMB)
	VALUES (
		'Log',
		@defaultLog,
		(SELECT TOP (1) CONVERT(BIGINT, result) /1024/1024 FROM @tmpDiskSpace)
	)
	DELETE FROM @tmpDiskSpace
	
	-- SQL Dump
	/*
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Collecting sql dump disk free space'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @xpCMD = '
		powershell.exe -c "Get-Volume -FilePath ''' + CONVERT(VARCHAR(MAX), @SQLDumpDir) + ''' | Select -ExpandProperty SizeRemaining"
	';

	INSERT INTO @tmpDiskSpace (result)
	EXEC xp_cmdshell @xpCMD

	-- Add to table SQLDump FreeSpace
	
	INSERT INTO @diskFreeSpace (DriveType, Drive, FreeSpaceMB)
	VALUES (
		'SQLDump',
		@SQLDumpDir,
		(SELECT TOP (1) CONVERT(BIGINT, result) /1024/1024 FROM @tmpDiskSpace)
	)
	DELETE FROM @tmpDiskSpace
	*/
	
	-- Disable xp_cmdshell (if not enabled by default)
	IF(@chkAdvOptions = 0)
	BEGIN
		EXEC sp_configure 'xp_cmdshell', 0;
		RECONFIGURE;
	END

	-- Disable Advanced Options (if not enabled by default)
	IF(@chkAdvOptions = 0)
	BEGIN
		EXEC sp_configure 'show advanced options', 0;
		RECONFIGURE;
	END

	-- Data Disk Free Space Summary
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Data Disk Free Space (MB): '+ (SELECT CONVERT(NVARCHAR(MAX), FreeSpaceMB) FROM @diskFreeSpace WHERE DriveType = 'Data')
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	-- Log Disk Free Space Summary
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Log Disk Free Space (MB): '+ (SELECT CONVERT(NVARCHAR(MAX), FreeSpaceMB) FROM @diskFreeSpace WHERE DriveType = 'Log')
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	-- Compare free size vs db Size
	--Data
	IF NOT EXISTS (
		SELECT 1
		FROM @diskFreeSpace
		WHERE FreeSpaceMB IS NOT NULL
		AND FreeSpaceMB > @sourceDataSize
		AND DriveType = 'Data'
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Not enough disk space on data drive for restore - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		SET @returnCode = 20
		RETURN @returnCode;
	END

	-- Log
	IF NOT EXISTS (
		SELECT 1
		FROM @diskFreeSpace
		WHERE FreeSpaceMB IS NOT NULL
		AND FreeSpaceMB > @sourceLogSize
		AND DriveType = 'Log'
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Not enough disk space on log drive for restore - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		SET @returnCode = 21
		RETURN @returnCode;
	END
	
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Identify database copy method'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	-- Calculate Backup age to negative number
	SET @backupAge = @backupAge * -1

	-- If there is a backup software name specified
	IF((@backupSoftwareName IS NOT NULL) AND (REPLACE(@backupSoftwareName, ' ', '') != ''))
	BEGIN
		-- Check if there is a full backup 
		IF EXISTS (
			SELECT TOP (1) 1
			FROM msdb.dbo.backupset a
			INNER JOIN msdb.dbo.backupmediaset b
				ON a.media_set_id = b.media_set_id
			WHERE a.type = 'D'
			AND a.database_name = @dbName
			AND a.backup_finish_date >= DATEADD(DAY, @backupAge, GETDATE())
			AND b.software_name = @backupSoftwareName
			ORDER BY a.backup_finish_date DESC
		)
		BEGIN
			-- Set Clone Method value
			SET @cloneMethod = 'CopyBackup'
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Third party backup software full backup entry found - will perform a copy-only full backup to SQL Dump drive'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END
		ELSE
		BEGIN
			SET @cloneMethodRecheck = 1
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Third party backup software full backup entry not found - checking if native backup set exists'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END
	END

	IF(((@backupSoftwareName IS NULL) AND (REPLACE(@backupSoftwareName, ' ', '') = '')) OR @cloneMethodRecheck = 1)
	BEGIN
		IF EXISTS(
			SELECT TOP (1) b.physical_device_name
			FROM msdb.dbo.backupset a
			INNER JOIN msdb.dbo.backupmediafamily b
				ON a.media_set_id = b.media_set_id
			WHERE a.type = 'D'
			AND a.database_name = @dbName
			AND a.backup_finish_date >= DATEADD(DAY, @backupAge, GETDATE())
			ORDER BY a.backup_finish_date DESC
		)
		BEGIN
			SET @cloneMethod = 'NativeBackup'
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | SQL Native Backup db full backup entry found - will perform a restore from existing backup file'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END
		ELSE
		BEGIN
			SET @cloneMethod = 'CopyBackup'
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | SQL Native Backup db full backup entry not found - will perform a copy-only full backup to SQL Dump drive'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END
	END

	-- IF restore from existing backup file - verify backup
	IF(@cloneMethod = 'NativeBackup')
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Verifying native full backup file'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;

		-- Set Backup filename
		SET @backupFilename = (
			SELECT TOP (1) b.physical_device_name
			FROM msdb.dbo.backupset a
			INNER JOIN msdb.dbo.backupmediafamily b
				ON a.media_set_id = b.media_set_id
			WHERE a.type = 'D'
			AND a.database_name = @dbName
			AND a.backup_finish_date >= DATEADD(DAY, @backupAge, GETDATE())
			ORDER BY a.backup_finish_date DESC
		)

		BEGIN TRY
			RESTORE VERIFYONLY
			FROM DISK = @backupFilename WITH  FILE = 1
			--if everything ok
			SET @cloneMethod = 'NativeBackup'
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Backup file is valid'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END TRY
		BEGIN CATCH
			SET @cloneMethod = 'CopyBackup'
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Backup file is not valid - will perform manual copy-only backup to SQL Dump drive'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END CATCH
	END

	--#############################################################
	-- PROCESSING BACKUP

	IF(@cloneMethod = 'CopyBackup')
	BEGIN
		-- Set backup filename 
		SET @backupFilename = @SQLDumpDir + @dbName + '_' + 'FULL' + '_' + @thisDate + '_' + @thisTime + '.bak'
		
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Performing copy-only full backup of database "' + @dbName + '" to disk: ' + @backupFilename
		RAISERROR(@msg, 10, 1) WITH NOWAIT;

		SET @cmd = '
			BACKUP DATABASE [' + @dbName + '] 
			TO  DISK = N''' + @backupFilename + ''' 
			WITH  COPY_ONLY, NOFORMAT, NOINIT, SKIP, NOREWIND, NOUNLOAD, COMPRESSION
		';

		BEGIN TRY
			EXEC sp_executesql @cmd
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Full Backup of database "' + @dbName +'" finished'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Full Backup of database "' + @dbName +'" failed' + CHAR(13) + CHAR(10) +
				'Error no: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
				'Message: ' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) +
				'Possible causes:' + CHAR(13) + CHAR(10) +
				'- Cannot access SQL Dump directory' + CHAR(13) + CHAR(10) +
				'- Not enough disk space on SQL Dump directory'
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @returnCode = 30
			RETURN @returnCode;
		END CATCH
	END

	--################################################################
	-- Processing Restore
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Performing restore of database "' + @targetDBName + '" from file: ' + @backupFilename
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	SET @cmd = '
		RESTORE FILELISTONLY
		FROM DISK = N''' + @backupFilename + ''' WITH FILE = 1
	';

	BEGIN TRY
		INSERT INTO @fileList
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not read database file configuration from file: ' + @backupFilename + CHAR(13) + CHAR(10) +
			'Error no: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
			'Message: ' + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		SET @returnCode = 40
		RETURN @returnCode;
	END CATCH

	-- Design Restore Script
	SET @cmd = '
		USE [master];
		RESTORE DATABASE [' + @targetDBName + ']
		FROM DISK = N''' + @backupFilename + ''' WITH FILE = 1,
	'

	DECLARE @logicalFilename NVARCHAR(MAX)
	DECLARE @tmpRestorePath NVARCHAR(MAX)
	DECLARE curFilesData CURSOR FOR 
		SELECT LogicalName FROM @fileList WHERE Type = 'D' ORDER BY FileId ASC

	-- Loop through all data files
	OPEN curFilesData

	FETCH NEXT FROM curFilesData INTO @logicalFilename

	WHILE @@FETCH_STATUS = 0
	BEGIN

		IF(@dbFileCount = 1)
		BEGIN
			SET @tmpRestorePath = @defaultData + @targetDBName + '_' + CONVERT(NVARCHAR(8), @dbFileCount) + '.mdf'
			SET @cmd += 'MOVE N''' + @logicalFilename + ''' TO N''' + @tmpRestorePath + ''',' + CHAR(13) + CHAR(10)
		END
		ELSE
		BEGIN
			SET @tmpRestorePath = @defaultData + @targetDBName + '_' + CONVERT(NVARCHAR(8), @dbFileCount) + '.ndf'
			SET @cmd += 'MOVE N''' + @logicalFilename + ''' TO N''' + @tmpRestorePath + ''',' + CHAR(13) + CHAR(10)
		END

		SET @dbFileCount += 1
		FETCH NEXT FROM curFilesData INTO @logicalFilename
	END

	CLOSE curFilesData
	DEALLOCATE curFilesData

	-- Reset File Counter
	SET @dbFileCount = 1

	-- Loop through all Log Files
	DECLARE curFilesLog CURSOR FOR
		SELECT LogicalName FROM @fileList WHERE Type = 'L' ORDER BY FileId ASC 

	OPEN curFilesLog

	FETCH NEXT FROM curFilesLog INTO @logicalFilename

	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @tmpRestorePath = @defaultLog + @targetDBName + '_log_' + CONVERT(NVARCHAR(8), @dbFileCount) + '.ldf'
		SET @cmd += 'MOVE N''' + @logicalFilename + ''' TO N''' + @tmpRestorePath + ''',' + CHAR(13) + CHAR(10)

		SET @dbFileCount += 1
		FETCH NEXT FROM curFilesLog INTO @logicalFilename
	END

	CLOSE curFilesLog
	DEALLOCATE curFilesLog

	-- Finalize Restore Script
	SET @cmd += 'NOUNLOAD'

	-- Restore database
	BEGIN TRY
		EXEC sp_executesql @cmd
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Database restore successfull'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Database restore failed' + CHAR(13) + CHAR(10) +
			'Error no: ' + CONVERT(NVARCHAR(MAX), ERROR_NUMBER()) + CHAR(13) + CHAR(10) +
			'Message: ' + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		SET @returnCode = 41
		RETURN @returnCode;
	END CATCH

	RETURN @returnCode;
END
