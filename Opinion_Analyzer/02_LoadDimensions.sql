-- ============================================================
-- Script: 02_LoadDimensions.sql
-- Carga todas las dimensiones + queries de verificación
-- ============================================================

USE OpinionesAnalitica;
GO

-- ══════════════════════════════════════════════════════════
-- 1. DimSentiment  (valores fijos — 4 filas)
-- ══════════════════════════════════════════════════════════
PRINT '>> Cargando DimSentiment...';

INSERT INTO DimSentiment (SentimentLabel)
SELECT v.label
FROM (VALUES
    ('Positivo'), ('Negativo'), ('Neutro'), ('Sin clasificar')
) AS v(label)
WHERE NOT EXISTS (
    SELECT 1 FROM DimSentiment WHERE SentimentLabel = v.label
);

SELECT * FROM DimSentiment ORDER BY SentimentKey;
GO

-- ══════════════════════════════════════════════════════════
-- 2. DimSource  (3 canales de origen)
-- ══════════════════════════════════════════════════════════
PRINT '>> Cargando DimSource...';

INSERT INTO DimSource (SourceName, SourceType, LoadDate)
SELECT src.SourceName, src.SourceType, GETDATE()
FROM (VALUES
    ('Encuestas Internas', 'encuesta'),
    ('Reseñas Web',        'web'),
    ('Redes Sociales',     'social')
) AS src(SourceName, SourceType)
WHERE NOT EXISTS (
    SELECT 1 FROM DimSource WHERE SourceName = src.SourceName
);

SELECT * FROM DimSource ORDER BY SourceKey;
GO

-- ══════════════════════════════════════════════════════════
-- 3. DimDate  (rango 2024-01-01 a 2026-12-31)
-- ══════════════════════════════════════════════════════════
PRINT '>> Cargando DimDate (2024–2026)...';

DECLARE @cur  DATE = '2024-01-01';
DECLARE @end  DATE = '2026-12-31';

WHILE @cur <= @end
BEGIN
    DECLARE @key INT = CONVERT(INT, FORMAT(@cur,'yyyyMMdd'));
    IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey = @key)
        INSERT INTO DimDate (DateKey, FullDate, Year, Quarter, Month, MonthName, Week, Day)
        VALUES (
            @key, @cur,
            YEAR(@cur),
            DATEPART(QUARTER,  @cur),
            MONTH(@cur),
            DATENAME(MONTH,    @cur),
            DATEPART(WEEK,     @cur),
            DAY(@cur)
        );
    SET @cur = DATEADD(DAY, 1, @cur);
END;

SELECT COUNT(*)        AS TotalFechas,
       MIN(FullDate)   AS Desde,
       MAX(FullDate)   AS Hasta
FROM DimDate;
GO

-- ══════════════════════════════════════════════════════════
-- 4. DimProduct  (desde Staging_Opinions)
-- ══════════════════════════════════════════════════════════
PRINT '>> Cargando DimProduct desde Staging...';

MERGE DimProduct AS tgt
USING (
    SELECT DISTINCT
        ProductId,
        COALESCE(MAX(ProductId), 'Sin nombre') AS ProductName,
        'General'                               AS Category,
        0.00                                    AS Price
    FROM Staging_Opinions
    WHERE ProductId IS NOT NULL
    GROUP BY ProductId
) AS src ON tgt.ProductId = src.ProductId
WHEN NOT MATCHED THEN
    INSERT (ProductId, ProductName, Category, Price)
    VALUES (src.ProductId, src.ProductName, src.Category, src.Price);

SELECT * FROM DimProduct ORDER BY ProductKey;
GO

-- ══════════════════════════════════════════════════════════
-- 5. DimCustomer  (desde Staging_Opinions)
-- ══════════════════════════════════════════════════════════
PRINT '>> Cargando DimCustomer desde Staging...';

MERGE DimCustomer AS tgt
USING (
    SELECT DISTINCT
        CustomerId,
        COALESCE(MAX(CustomerEmail), 'sin-email@nd.com') AS Email,
        COALESCE(MAX(CustomerName),  'Desconocido')      AS CustomerName,
        COALESCE(MAX(Country),       'N/A')              AS Country
    FROM Staging_Opinions
    WHERE CustomerId IS NOT NULL
    GROUP BY CustomerId
) AS src ON tgt.CustomerId = src.CustomerId
WHEN NOT MATCHED THEN
    INSERT (CustomerId, Email, CustomerName, Country)
    VALUES (src.CustomerId, src.Email, src.CustomerName, src.Country);

SELECT * FROM DimCustomer ORDER BY CustomerKey;
GO

-- ══════════════════════════════════════════════════════════
-- Resumen general
-- ══════════════════════════════════════════════════════════
PRINT '>> Resumen de carga:';
SELECT 'DimSentiment' AS Tabla, COUNT(*) AS Registros FROM DimSentiment
UNION ALL
SELECT 'DimSource',   COUNT(*) FROM DimSource
UNION ALL
SELECT 'DimDate',     COUNT(*) FROM DimDate
UNION ALL
SELECT 'DimProduct',  COUNT(*) FROM DimProduct
UNION ALL
SELECT 'DimCustomer', COUNT(*) FROM DimCustomer;
GO
