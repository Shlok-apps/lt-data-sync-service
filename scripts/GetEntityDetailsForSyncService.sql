USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetEntityDetailsForSyncService]    Script Date: 2025-07-17 18:28:46 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetEntityDetailsForSyncService]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        EN.[ID],
        (EN.Code + '-' + EN.[Name]) AS EntityName,
        BTE.IC,
        BTE.SBG,
        BTE.BU,
        BTE.Cluster
    FROM PI_Entity EN
    CROSS APPLY dbo.GetAllBranchTypeColumnsForEntity(EN.ID) BTE;
END
GO


