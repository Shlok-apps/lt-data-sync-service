USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetUserDetailsForSyncService]    Script Date: 2025-07-18 10:34:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[GetUserDetailsForSyncService]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        EB.EmployeeId,
        (EB.EmployeeCode + '-' + EB.EmployeeName) AS EmployeeName,
        (EN.NIF + '-' + EN.[Name]) AS Jobsite,
        BT.IC,
        BT.SBG,
        BT.BU,
        BT.Cluster
    FROM CH_tblEmployeeBasics EB
    CROSS APPLY dbo.GetAllBranchTypeColumns(EB.EmployeeId) BT
    INNER JOIN PI_Entity EN ON EN.ID = BT.EntityId
    CROSS APPLY dbo.GetAllBranchTypeColumnsForEntity(EN.ID) BTE;
END
GO


