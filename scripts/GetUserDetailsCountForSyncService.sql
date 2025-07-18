USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetUserDetailsCountForSyncService]    Script Date: 2025-07-18 10:33:38 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[GetUserDetailsCountForSyncService]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT(*) AS TotalUserCount
    FROM CH_tblEmployeeBasics EB
    CROSS APPLY dbo.GetAllBranchTypeColumns(EB.EmployeeId) BT
    INNER JOIN PI_Entity EN ON EN.ID = BT.EntityId
    CROSS APPLY dbo.GetAllBranchTypeColumnsForEntity(EN.ID) BTE;
END
GO


