CREATE TABLE [dbo].[EmployeeData] (

	[EmployeeID] int NOT NULL, 
	[FirstName] varchar(50) NOT NULL, 
	[LastName] varchar(50) NOT NULL, 
	[Address] varchar(255) NULL, 
	[Country] varchar(50) NULL, 
	[EmailAddress] varchar(100) NOT NULL, 
	[PhoneNumber] varchar(20) NULL, 
	[DateOfBirth] date NULL, 
	[DateHired] date NULL, 
	[Department] varchar(50) NULL, 
	[JobTitle] varchar(100) NULL, 
	[Salary] decimal(10,2) NULL, 
	[ManagerID] int NULL, 
	[DepartmentPhoneNumber] varchar(20) NULL, 
	[EmergencyContactName] varchar(100) NULL, 
	[EmergencyContactPhone] varchar(20) NULL
);