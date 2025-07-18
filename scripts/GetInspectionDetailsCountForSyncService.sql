USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetInspectionDetailsCountForSyncService]    Script Date: 2025-07-18 10:30:53 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetInspectionDetailsCountForSyncService]
    @fromTime DATETIME,
    @toTime DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT(1) AS TotalCount
    FROM
    (
        SELECT PB.ProjectId
        FROM CH_tblProjectBasics PB
        INNER JOIN CH_tblProjectObjects PO ON PO.ProjectId = PB.ProjectId AND PO.GoObjectType = 'O'
        INNER JOIN CH_tblProjectServices PS ON PS.ProjectObjectId = PO.ProjectObjectId
        INNER JOIN CH_tblAgendamentor AG ON AG.ProjectId = PB.ProjectId AND AG.IsPrimary = 1
        INNER JOIN PI_Establishment ES ON ES.ID = PO.EstablishmentID
        INNER JOIN PI_Entity EN ON EN.ID = ES.ID_PI_Entity
        INNER JOIN PI_StatusInformation SI ON SI.ID = PB.StatusID_PI_StatusInformation
        INNER JOIN CH_tblEmployeeBasics EBC ON EBC.EmployeeId = PB.CreatorUserId
        INNER JOIN PI_TblMasterServiceType MST ON MST.PIServiceTypeId = PB.PIServiceTypeID
        INNER JOIN CH_tblMastserviceType MT ON MT.ServiceTypeId = PS.ServiceId
         
WHERE EBC.IsActive = 1 AND MST.ProductType IN ('INSPECTION')
AND DATEADD(MINUTE, DATEDIFF(MINUTE, 0, PB.CreationDate), 0)
                BETWEEN DATEADD(MINUTE, DATEDIFF(MINUTE, 0, @fromTime), 0)
                AND DATEADD(MINUTE, DATEDIFF(MINUTE, 0, @toTime), 0)
    ) AS SubQuery
END
GO


