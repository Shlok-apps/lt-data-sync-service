USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetTaskDetailsCountForSyncService]    Script Date: 2025-07-18 10:32:22 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[GetTaskDetailsCountForSyncService]
    @fromTime DATETIME,
    @toTime DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT(*) AS TotalCount
    FROM TM_Task TM
    INNER JOIN PI_TaskType TT ON TT.ID = TM.ID_PI_TaskType
    INNER JOIN TM_Status TS ON TS.ID = TM.ID_TM_Status
    INNER JOIN CH_tblEmployeeBasics EB ON EB.EmployeeId = TM.CreateUserId
    INNER JOIN CH_tblEmployeeBasics EBA ON EBA.EmployeeId = TM.ID_User_Internal_Owner
    INNER JOIN PI_Entity EN ON EN.ID = TM.ID_PI_Entity
    INNER JOIN PI_TblMasterServiceType MST ON MST.PIServiceTypeId = TM.PIServiceTypeId_PI_tblMasterServicetype
    LEFT JOIN PI_LookupValues LV ON LV.ID = TM.Priority
    LEFT JOIN PI_LookupValues LV1 ON LV1.ID = TM.Severity
    WHERE 
        TM.PublishTask = 1
        AND TM.ID_PI_Entity IS NOT NULL
        AND DATEADD(MINUTE, DATEDIFF(MINUTE, 0, TM.CreationDate), 0) >= DATEADD(MINUTE, DATEDIFF(MINUTE, 0, @fromTime), 0)
        AND DATEADD(MINUTE, DATEDIFF(MINUTE, 0, TM.CreationDate), 0) <= DATEADD(MINUTE, DATEDIFF(MINUTE, 0, @toTime), 0)
        AND TM.CreateUserId <> 1616;
END
GO


