USE [ConquerBNFBackup_UAT_June11th-2025-7-16-13-2]
GO

/****** Object:  StoredProcedure [dbo].[GetInspectionDetailsForSyncService]    Script Date: 2025-07-18 10:31:47 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[GetInspectionDetailsForSyncService]
    @fromTime DATETIME,
    @toTime DATETIME
AS
BEGIN
    SET NOCOUNT ON;

SELECT ProjectId, ProjectCode, ScheduleDate, CreationDate, ClosedDate, SectorCode, Sector, Establishment, InspectionStatus,  Product, Checklist, CreatedUser,
PendingWith,  0 as Score, '' as Result
FROM
(Select PB.ProjectId, PB.ProjectCode,
AG.ScheduleDate,
PB.[CreationDate],
PB.[ClosedDate],
EN.NIF as SectorCode,
EN.[Name] as Sector,
ES.[NameofEstablishment] as Establishment,
CASE WHEN GroupCode = 'NEW' THEN 'New' WHEN GroupCode = 'CAN' THEN 'Cancelled' WHEN GroupCode = 'CL' THEN 'Closed' WHEN GroupCode = 'COM' THEN 'Closed'
WHEN GroupCode = 'INP' THEN 'Inprogress' ELSE GroupCode END as [InspectionStatus],
MST.[ProductType] as ProductType,
MST.[Description] as Product,
MT.[ServiceType] as Checklist,
EBC.EmployeeName as CreatedUser,
(
        SELECT TOP 1 EB.EmployeeName
FROM PI_BPMActivitiesExecution B
INNER JOIN CH_tblEmployeeBasics EB
ON EB.EmployeeId = B.EmployeeId_CH_tblEmployeeBasics
WHERE B.ProjectId_CH_tblProjectBasics = PB.ProjectId
AND B.ID_PI_BPMActivityStatus IN (1, 2, 7, 6)
AND [Type] = 'T'
ORDER BY
    CASE
        WHEN B.ID_PI_BPMActivityStatus = 7 THEN 0
        WHEN B.ID_PI_BPMActivityStatus = 6 THEN 1
        ELSE 2
    END,
    B.ID_PI_BPMActivityStatus
    ) as PendingWith
from CH_tblProjectBasics PB
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
) T

END
GO


