--	drop database [VIVAR]

--1.- Crear una Base de datos, el nombre debe ser su primer apellido. 
-- Características de BD:
-- MDF: SIZE = 92160KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB
-- LDF:  SIZE = 14475KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB
-- NDF FILEGROUP [INDICES]:  SIZE = 14475KB , MAXSIZE = UNLIMITED,
-- FILEGROWTH = 65536KB
-- EL NOMBRE DE MDF Y LDF PUEDE SER CUALQUIERA.

CREATE DATABASE CARGA
on primary(
 Name = 'carga_pri',
 FileName ='D:\_Exa.MDF',
 SIZE=92160 KB,
 MAXSIZE = UNLIMITED,
 FILEGROWTH= 65536 KB
)
Log On(
 Name = 'rcarga_log',
 FileName ='D:\_Exa.LDF',
 SIZE=14475 KB,
 MAXSIZE = UNLIMITED,
 FILEGROWTH= 65536 KB
)
GO
ALTER DATABASE CARGA 
ADD FILE(
	 Name = 'rcarga_data',
	 FileName ='D:\_Exa.NDF',
	 SIZE=14475 KB,
	 MAXSIZE = UNLIMITED,
	 FILEGROWTH= 65536 KB
)
--2.- Desarrollo de  ETL: Cargar los siguientes archivos a la base de datos creada usando Python (Visual Studio Code, Spyder, Jupyter, Etc).
--Archivo: Resultados.csv
--Nota: No Manipular el archivo manualmente.
--La tabla a cargar debe tener las siguientes características:
--
--Esquema: Base
--Nombre Tabla: Resultados
--Índice: Clustered (Fecha)
--Nombre Índice: Cls_Idx_Fecha
--
--Agregar 2 columnas Adicionales a todas las tablas creadas con el nombre del archivo y la fecha hora de carga.
go
USE CARGA
go
Create SCHEMA Base
go
create TABLE Base.Resultados
(
	"Fecha" "datetime" null,
	"Alcance" "int" null,
	"Tipo" nvarchar(255)null
)
go
create index Cls_Idx_Fecha on Base.Resultados(Fecha)
go
	select *from  Base.Resultados
go
-- 3.- Realizar las siguientes Querys (Transact-SQL):
-- 	Crear un Procedimiento almacenado que de como output el Alcance de la página de Facebook según las fecha de inicio
--	y fin se que ingrese como parámetro. (Tabla:Base.Resultados)
-- Nombre del Procedimiento:
--  uSp_ListaAlcance_Facebook (@FechaInicio Datetime, FechaFin Datetime)

-- 	Crear una función  que reciba como parámetro la fecha y devuelva la cantidad de Alcance de Instagram en la fecha especificada. 
--	(Tabla:Base.Resultados)
-----------------------------------------------------------------------------
Create PROC Base.uSp_ListaAlcance_Facebook
	@FechaInicio DATETIME,@FechaFin DATETIME
AS
	SELECT Alcance  FROM Base.Resultados
		WHERE fecha BETWEEN @FechaInicio AND @FechaFin
go
-----------------------------------------------------------------------------
create function Base.Resultado
(	@Fecha DATETIME	)
	returns int 
as 
BEGIN
DECLARE @cantidad int
	select @cantidad = Alcance  FROM Base.Resultados WHERE fecha = @Fecha
return @cantidad
END
-----------------------------------------------------------------------------