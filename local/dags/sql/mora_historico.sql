CREATE OR REPLACE PROCEDURE `teco-dev-cdp-056e.test_gian.mora_historico`()
/*
Script que genera la audiencia de mora diara que se inserta sobre la tabla audiencia_mora_fan_historico
las particiones duran 45 dias.
Esta tabla se envía a looker y a responsys con el objetivo de comunicar a los clientes morosos
*/

-- Utilizamos la ultima fecha de carga presente en FTH Parque
BEGIN
DECLARE fecha_entidad DATE;
SET @@dataset_project_id = 'teco-dev-cdp-056e';
SET @@dataset_id = 'test_gian';
SET fecha_entidad = (
  SELECT MAX(PARSE_DATE('%Y%m%d', CAST(partition_id AS STRING))) FROM `teco-prod-edw-5e1b.ue4_prod_edw_pub_gcp.INFORMATION_SCHEMA.PARTITIONS`
  WHERE table_name = 'FTH_Parque'
  AND partition_id <> '__NULL__'
  AND total_rows > 100000000
);

-- Creacion de la audienci inicial de mora
CREATE OR REPLACE TEMP TABLE mora AS (
  SELECT  
    OBJECT_ID,
    CUENTA_CODE AS REFERENTE_PAGO_FAN, 
    TIPO_MORA_HITO,
    ULT_ACCION_FECHA_EXE,
    CUENTA_NPLAY,
    CUENTA_SEGMENTACION,
    CUENTA_Q_FIJA_TV,			
    CUENTA_Q_FIJA_INTERNET,					
    CUENTA_Q_FIJA_TELEFONIA AS CUENTA_Q_FIJA_TOIP, #cambiado el 18/07/2023		
    CUENTA_Q_MOVIL_ABONO,					
    CUENTA_Q_MOVIL_PRE,				
    CUENTA_Q_FIJA_BUNDLE,
    OPEN_AMOUNT,
    GROUP_CODE AS COLLECCION_GRUPO,
    GROUP_VERSION AS LINEA_VERSION,
    CUENTA_Q_FIJA_FLOW
  FROM `teco-prod-edw-5e1b.ue4_prod_edw_pub_gcp.FT_Morosidad`
  WHERE 
    UPPER(TRIM(DUNNING_FLAG_DES)) = 'ACTIVO'
    AND UPPER(TRIM(EXEC_STATUS_DES)) = 'EJECUTANDO'
    AND  OPEN_AMOUNT > 150
);
--Traemos los datos de los clientes
CREATE OR REPLACE TEMP TABLE parque AS(
  SELECT 
    DISTINCT
    Cliente.CuentaCodigo  AS REFERENTE_PAGO_FAN,
    UPPER(Producto.ProductoTipoNombre) AS TIPO_PRODUCTO,
    ProductoAdquiridoNumeroLinea  AS LINEA,
    COALESCE(
      CASE WHEN Cliente.ClienteEmail <> '(no informado)' THEN Cliente.ClienteEmail END,
      CASE WHEN Cliente.CuentaEmail <> '(no informado)' THEN Cliente.CuentaEmail END,
      CASE WHEN Cliente.PersonaEmail <> '(no informado)' THEN Cliente.PersonaEmail END
    ) AS EMAIL,
    LEFT(Cliente.PersonaGenero, 1) AS GENERO,
    Cliente.PersonaDocumentoNumero AS DOCUMENTO, 
    Cliente.PersonaTipoDocumentoNombre AS TIPO_DOCUMENTO
  FROM  `teco-prod-edw-5e1b.ue4_prod_edw_pub_gcp.FTH_Parque` 
  WHERE 
    (FechaEntidad) = fecha_entidad
    AND  ( Marcas.ProductoAdquiridoMarcaPrincipal = TRUE
    AND Marcas.ProductoAdquiridoMarcaParqueActivo = TRUE
    AND UPPER(Producto.ProductoTipoNombre) IN ('PLAN POSPAGO', 'PLAN HIBRIDO', 'PLAN PREPAGO', 'INTERNET', 'TV', 'TELEFONIA')
    OR (
      UPPER(ProductoAdquiridoNombre) LIKE '%FLOW FLEX%'
      AND Marcas.ProductoAdquiridoMarcaParqueActivo = TRUE
      AND Marcas.ProductoAdquiridoMarcaPrincipal = FALSE
      AND UPPER(Producto.ProductoFamiliaNombre) = 'FIJA'
      AND UPPER(ProductoAdquiridoTipo) = 'TV'
    )
    )
    AND  (ProductoAdquiridoEstadoNombre = 'ACTIVO' 
      OR (ProductoAdquiridoEstadoNombre = 'SUSPENDIDO' AND Suspensiones.MotivoSuspensionUltimaNombre = 'DEGRADACION')
      OR (ProductoAdquiridoEstadoNombre = 'SUSPENDIDO' AND Suspensiones.MotivoSuspensionUltimaNombre = '(no informado)')
    )
    AND Producto.ProductoTipoNombre <> 'BUNDLE'
);
-- Realizamos el cruce de parque con mora y sumamos los datos de financiacion
CREATE OR REPLACE TEMP TABLE mora_parque AS(
WITH financiacion AS(
  SELECT
    DISTINCT
    ACCT_CODE AS REFERENTE_PAGO_FAN, -- CRUZAR ACCT 
    1 AS FLG_DPF -- FLAG A AGREGAR EN LA AUDIENCIA DE MORA
  FROM
  `teco-prod-edw-5e1b.ue4_prod_edw_pub_gcp.FT_PlanFinanciacion`
  WHERE
  TIMESTAMP_TRUNC(CREATE_DATE, DAY) = TIMESTAMP(CURRENT_DATE()-1) 
  AND CYCLE_STATUS = 'N'
)
SELECT * FROM parque
RIGHT JOIN mora USING(REFERENTE_PAGO_FAN)
LEFT JOIN financiacion USING(REFERENTE_PAGO_FAN)
WHERE TIPO_PRODUCTO IS NOT NULL -- Comentar esta linea para debugear posibles casos faltantes que se filtren en parque
);

-- Creacion del URL de pago personalizado
CREATE OR REPLACE TEMP TABLE mora_url_pago AS(
WITH mapeo_tipo_documento AS (
    SELECT 'DOCUMENTO NACIONAL DE IDENTIDAD (DNI)' AS TIPO_DOCUMENTO, '004' AS code
    UNION ALL
    SELECT 'CEDULA DE IDENTIDAD (CID)' AS TIPO_DOCUMENTO, '1' AS code
    UNION ALL
    SELECT 'PASAPORTE' AS TIPO_DOCUMENTO, '7' AS code
    UNION ALL
    SELECT 'CLAVE UNICA DE IDENTIFICACION LABORAL (CUIL)' AS TIPO_DOCUMENTO, '9' AS code
    UNION ALL
    SELECT 'CLAVE UNICA DE IDENTIFICACION TRIBUTARIA (CUIT)' AS TIPO_DOCUMENTO, '010' AS code
    UNION ALL
    SELECT 'LIBRETA CIVICA (LC)' AS TIPO_DOCUMENTO, '5' AS code
    UNION ALL
    SELECT 'LIBRETA DE ENROLAMIENTO (LE)' AS TIPO_DOCUMENTO, '6' AS code
),
mora_code AS(
    SELECT
        *
    FROM mora_parque
    LEFT JOIN 
        mapeo_tipo_documento USING(TIPO_DOCUMENTO)
)

SELECT
  *,
  CASE
    WHEN tipo_producto IN ("PLAN POSPAGO", "PLAN HIBRIDO") THEN CONCAT('https://pagos.telecom.com.ar/pages/payment/phone/', LINEA)
    WHEN tipo_producto NOT IN ("PLAN PREPAGO", "PLAN HIBRIDO", "PLAN POSPAGO") THEN CONCAT('https://pagos.telecom.com.ar/pages/payment/cable/', GENERO, '/',
    IF (TIPO_DOCUMENTO LIKE '%DNI%',IF(GENERO = 'F', '014', code), code), '/', DOCUMENTO)
  ELSE NULL END AS URL_PAGO
FROM mora_code
);


-- Limpieza de datos y generacion de datos finales para el insert
INSERT INTO audiencia_mora_fan_historico
WITH mora_curated AS (
    SELECT
    CASE WHEN TIPO_PRODUCTO in ('PLAN HIBRIDO', 'PLAN PREPAGO', 'PLAN POSPAGO') THEN LINEA ELSE REFERENTE_PAGO_FAN END AS CUSTOMER_ID,
    REFERENTE_PAGO_FAN,
    EMAIL,
    OBJECT_ID,
    TIPO_MORA_HITO AS INSTANCIA_CBS_FAN,
    CAST(ULT_ACCION_FECHA_EXE AS DATE) AS FECHA_MORA_HITO_FAN,
    CUENTA_NPLAY,
    CUENTA_SEGMENTACION,
    CUENTA_Q_FIJA_TV,
    CUENTA_Q_FIJA_INTERNET,
    CUENTA_Q_FIJA_TOIP,
    CUENTA_Q_MOVIL_ABONO,
    CUENTA_Q_MOVIL_PRE,
    CUENTA_Q_FIJA_BUNDLE,
    OPEN_AMOUNT,
    COLLECCION_GRUPO,
    LINEA_VERSION,
    CUENTA_Q_FIJA_FLOW,
    COALESCE(FLG_DPF, 0) AS FLG_DPF,
    URL_PAGO
    FROM mora_url_pago
)

SELECT 
DISTINCT
    CUSTOMER_ID,
    REFERENTE_PAGO_FAN,
    EMAIL,
    OBJECT_ID,
    INSTANCIA_CBS_FAN,
    FECHA_MORA_HITO_FAN,
    CUENTA_NPLAY,
    CUENTA_SEGMENTACION,
    ROUND 
    (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT END, 2) AS DEUDA_VENCIDA_FAN,  -- SALDO_VENCIDO
    ROUND 
    (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT * 0.3 END, 2) AS PAGO_MINIMO_FAN,  -- PAGO_MINIMO_FAN: 30% SALDO_VENCIDO (campo calculado)
    ROUND
    (((CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT END) - (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT * 0.3 END)) / 3, 2) AS PAGO_3_CUOTAS_FAN,   -- SALDO_VENCIDO menos PAGO_MINIMO_FAN, dividido 6 (campo calculado)
    ROUND
    (((CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT END) - (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT * 0.3 END)) / 6, 2) AS PAGO_6_CUOTAS_FAN,   -- SALDO_VENCIDO menos PAGO_MINIMO_FAN, dividido 6 (campo calculado)
    ROUND
    (((CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT END) - (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT * 0.3 END)) / 9, 2) AS PAGO_9_CUOTAS_FAN,   -- SALDO_VENCIDO menos PAGO_MINIMO_FAN, dividido 9 (campo calculado)
    ROUND
    (((CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT END) - (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT * 0.3 END)) / 12, 2) AS PAGO_12_CUOTAS_FAN, -- SALDO_VENCIDO menos PAGO_MINIMO_FAN, dividido 12 (campo calculado)
    ROUND
    (CASE WHEN COALESCE (OPEN_AMOUNT, 0) = 0 THEN 0 ELSE OPEN_AMOUNT / 12 END, 2) AS PAGO_TOTAL_12_CUOTAS_FAN, -- SALDO_VENCIDO dividido 12 (campo calculado)
    CUENTA_Q_FIJA_TV,
    CUENTA_Q_FIJA_INTERNET,
    CUENTA_Q_FIJA_TOIP,
    CUENTA_Q_MOVIL_ABONO,
    CUENTA_Q_MOVIL_PRE,
    CUENTA_Q_FIJA_BUNDLE,
    CURRENT_DATE() AS FECHA_FOTO,
    DATE(NULL) AS FECHA_PROCESO,
    NULL AS FL_ENVIO,
    URL_PAGO,
    COLLECCION_GRUPO,
    LINEA_VERSION,
    FLG_DPF,
    CUENTA_Q_FIJA_FLOW
FROM mora_curated;
END

