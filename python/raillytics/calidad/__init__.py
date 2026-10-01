"""Quality Gates del lake, lado Python (el framework principal es Scala/Spark:
raillytics.common.calidad.QualityGates y la app raillytics.calidad.QualityGatesApp).

Aquí solo vive lo que no puede hacer Spark: validar el fichero en el momento de la
descarga (raillytics.calidad.ficheros) y dejar esos resultados en la misma tabla
Parquet de trazabilidad que escribe Scala (raillytics.calidad.registro).
"""
from raillytics.calidad.registro import (  # noqa: F401
    RESULTADO_ERROR,
    RESULTADO_FALLO,
    RESULTADO_OK,
    SEVERIDAD_AVISO,
    SEVERIDAD_BLOQUEANTE,
    QualityGateError,
    ResultadoGate,
    registrar_calidad,
)
