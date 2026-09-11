Después del dbt build limpio, corré la verificación que quedó pendiente:

sql
SELECT COUNT(*) AS filas, COUNT(DISTINCT _EXTRACT_DATE) AS dias
FROM AP_ANALYTICS.RAW.FBL1N_ITEMS;

SELECT COUNT(*) AS docs_con_varias_versiones
FROM (SELECT BELNR FROM AP_ANALYTICS.RAW.FBL1N_ITEMS GROUP BY BELNR HAVING COUNT(*) >