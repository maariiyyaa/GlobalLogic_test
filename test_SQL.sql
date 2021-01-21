-- TASK_1 CREATE TABLE
CREATE TABLE dbo.cities(
	id [int] IDENTITY(0,1) NOT NULL,
	[name] [char](50) NULL,
 CONSTRAINT PK_cities PRIMARY KEY CLUSTERED 
(
	id ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- TASK_2 CREATE TABLE
CREATE TABLE dbo.district(
	id [int] IDENTITY(0,1) NOT NULL,
	id_city [int] NULL,
	[name] [char](50) NULL,
 CONSTRAINT [PK_district] PRIMARY KEY CLUSTERED 
(
	id ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE dbo.district  WITH CHECK ADD  CONSTRAINT FK_district_cities FOREIGN KEY(id_city)
REFERENCES dbo.cities (id)
GO
ALTER TABLE dbo.district CHECK CONSTRAINT FK_district_cities
GO

--TASK_1 INSERT VALUES INTO cities TABLE
INSERT INTO cities([name])
VALUES ('Kyiv'),
	   ('Chernivtsi'),
	   ('Kropyvnytskyi'),
	   ('Odesa'),
	   ('Kharkiv'),
	   ('Obuhiv'),
	   ('Lviv'),
	   ('Lutsk');
GO

--TASK_1 INSERT VALUES INTO district TABLE
INSERT INTO district(id_city, [name])
VALUES (0, 'Sviatoshinskii'),
	   (0, 'Obolonskii'),
	   (0, 'Solomianskii'),
	   (1, 'Sadhipskii'),
	   (2, 'Podillskii'),
	   (1, 'Shevchenkivskii'),
	   (6, 'Halytskii'),
	   (7, 'Lutska miskz rada');
GO

-- TASK_1 SELECTION
SELECT [name]
FROM cities
ORDER BY 1 ASC;
GO

-- TASK_2 SELECTION
SELECT d.id, d.[name], c.[name]
FROM 
	district d
LEFT JOIN 
	cities c
ON d.id_city = c.id
ORDER BY 2 ASC;