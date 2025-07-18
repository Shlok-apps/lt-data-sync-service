USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetEntityDetailsCountForSyncService]    Script Date: 2025-07-17 18:42:17 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetEntityDetailsCountForSyncService]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT(*) AS TotalEntityCount
    FROM PI_Entity EN
    CROSS APPLY dbo.GetAllBranchTypeColumnsForEntity(EN.ID) BTE;
END
GO


