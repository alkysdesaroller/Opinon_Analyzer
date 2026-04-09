-- ============================================================
-- Script: 01_CreateDimensions.sql
-- Proyecto: Sistema de Análisis de Opiniones de Clientes
-- Autor: Albín Natán López Sánchez — 2024-1116
-- Descripción: Crea las tablas de dimensiones y staging
--              del DataWarehouse (modelo estrella híbrido).
-- ============================================================

USE OpinionesAnalitica;
GO

-- ── Staging ────────────────────────────────────────────────
IF OBJECT_ID('Staging_Opinions', 'U') IS NULL
CREATE TABLE Staging_Opinions (
    StagingId       INT IDENTITY(1,1) PRIMARY KEY,
    SourceType      NVARCHAR(50),
    ProductId       NVARCHAR(50),
    CustomerId      NVARCHAR(50),
    CustomerEmail   NVARCHAR(150),
    CustomerName    NVARCHAR(150),
    Country         NVARCHAR(50),
    Rating          INT,
    CommentText     NVARCHAR(MAX),
    OpinionDate     DATETIME,
    Classification  NVARCHAR(50),
    Score           INT,
    Platform        NVARCHAR(50),
    Sentiment       NVARCHAR(50),
    LoadedAt        DATETIME DEFAULT GETDATE()
);
GO

-- ── DimSentiment ──────────────────────────────────────────
IF OBJECT_ID('DimSentiment', 'U') IS NULL
CREATE TABLE DimSentiment (
    SentimentKey    INT IDENTITY(1,1) PRIMARY KEY,
    SentimentLabel  NVARCHAR(50) NOT NULL
        CONSTRAINT CHK_Sentiment
        CHECK (SentimentLabel IN ('Positivo','Negativo','Neutro','Sin clasificar'))
);
GO

-- ── DimSource ─────────────────────────────────────────────
IF OBJECT_ID('DimSource', 'U') IS NULL
CREATE TABLE DimSource (
    SourceKey   INT IDENTITY(1,1) PRIMARY KEY,
    SourceName  NVARCHAR(100) NOT NULL,
    SourceType  NVARCHAR(50)  NOT NULL
        CONSTRAINT CHK_SourceType
        CHECK (SourceType IN ('encuesta','web','social')),
    LoadDate    DATETIME DEFAULT GETDATE()
);
GO

-- ── DimDate ───────────────────────────────────────────────
IF OBJECT_ID('DimDate', 'U') IS NULL
CREATE TABLE DimDate (
    DateKey     INT PRIMARY KEY,       -- YYYYMMDD
    FullDate    DATE  NOT NULL,
    Year        INT   NOT NULL,
    Quarter     INT   NOT NULL,
    Month       INT   NOT NULL,
    MonthName   NVARCHAR(20) NOT NULL,
    Week        INT   NOT NULL,
    Day         INT   NOT NULL
);
GO

-- ── DimProduct ────────────────────────────────────────────
IF OBJECT_ID('DimProduct', 'U') IS NULL
CREATE TABLE DimProduct (
    ProductKey  INT IDENTITY(1,1) PRIMARY KEY,
    ProductId   NVARCHAR(50)  NOT NULL UNIQUE,
    ProductName NVARCHAR(150) NOT NULL,
    Category    NVARCHAR(100),
    Price       DECIMAL(10,2)
);
GO

-- ── DimCustomer ───────────────────────────────────────────
IF OBJECT_ID('DimCustomer', 'U') IS NULL
CREATE TABLE DimCustomer (
    CustomerKey  INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId   NVARCHAR(50)  NOT NULL UNIQUE,
    Email        NVARCHAR(150) NOT NULL,
    CustomerName NVARCHAR(150),
    Country      NVARCHAR(50)
);
GO

-- ── FactOpinions ──────────────────────────────────────────
IF OBJECT_ID('FactOpinions', 'U') IS NULL
CREATE TABLE FactOpinions (
    OpinionKey   INT IDENTITY(1,1) PRIMARY KEY,
    ProductKey   INT NOT NULL REFERENCES DimProduct(ProductKey),
    CustomerKey  INT NOT NULL REFERENCES DimCustomer(CustomerKey),
    DateKey      INT NOT NULL REFERENCES DimDate(DateKey),
    SourceKey    INT NOT NULL REFERENCES DimSource(SourceKey),
    SentimentKey INT NOT NULL REFERENCES DimSentiment(SentimentKey),
    OpinionType  NVARCHAR(50)
        CONSTRAINT CHK_OpinionType CHECK (OpinionType IN ('encuesta','review','social')),
    Rating       INT CHECK (Rating BETWEEN 1 AND 5),
    CommentText  NVARCHAR(MAX)
);
GO

PRINT 'Tablas creadas correctamente.';
GO
