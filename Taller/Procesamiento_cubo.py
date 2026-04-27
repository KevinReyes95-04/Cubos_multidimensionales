"""
Procesamiento de cubo ERA5 descargado desde CDS.

Objetivo:
1. Leer el NetCDF con evaluacion perezosa usando xarray + dask.
2. Auditar dimensiones, variables y CRS.
3. Aplicar algebra de mapas vectorizada.
4. Remuestrear de resolucion horaria a diaria.
5. Calcular maxima mensual y fecha de ocurrencia por pixel.
6. Exportar el resultado a mi_zona_procesada.nc.
"""

import os
import sysconfig
import xarray as xr


# -------------------------------------------------------------------------
# Configuracion espacial
# -------------------------------------------------------------------------
# En este equipo existe una instalacion de PostGIS/PostgreSQL que puede
# interferir con PROJ. Por eso se fuerza el uso del proj.db de rasterio.
proj_data_dir = os.path.join(sysconfig.get_paths()["purelib"], "rasterio", "proj_data")
os.environ["PROJ_LIB"] = proj_data_dir
os.environ["PROJ_DATA"] = proj_data_dir

import rioxarray  # noqa: E402


# -------------------------------------------------------------------------
# Rutas de entrada y salida
# -------------------------------------------------------------------------
base_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
data_dir_candidates = [
    os.path.abspath(os.path.join(base_dir, "..", "..", "..", "data_heavy")),
    os.path.abspath(os.path.join(os.getcwd(), "data_heavy")),
    os.path.abspath(os.path.join(os.getcwd(), "..", "data_heavy")),
]
data_dir = next(path for path in data_dir_candidates if os.path.exists(path))

input_file = os.path.join(data_dir, "era5_2m_temperature_2025_04_americas.nc")
output_file = os.path.join(data_dir, "mi_zona_procesada.nc")


# -------------------------------------------------------------------------
# 1. Lectura lazy con xarray + dask
# -------------------------------------------------------------------------
# chunks="auto" evita cargar todo el NetCDF inmediatamente en RAM.
# Las operaciones se preparan como tareas dask y se calculan al exportar.
ds = xr.open_dataset(input_file, chunks="auto")


# -------------------------------------------------------------------------
# 2. Auditoria del cubo
# -------------------------------------------------------------------------
print("\n=== RESUMEN DEL CUBO ===")
print(ds)

print("\n=== DIMENSIONES ===")
print(ds.sizes)

print("\n=== VARIABLES ===")
print(list(ds.data_vars))

# ERA5 viene en coordenadas geograficas lon/lat. Si el archivo no trae CRS
# explicito para rioxarray, lo declaramos como WGS84 (EPSG:4326).
ds = ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=False)
if ds.rio.crs is None:
    ds = ds.rio.write_crs("EPSG:4326", inplace=False)

print("\n=== CRS ACTUAL ===")
print(ds.rio.crs)


# -------------------------------------------------------------------------
# 3. Algebra de mapas vectorizada
# -------------------------------------------------------------------------
# Variable principal descargada:
# t2m = temperatura a 2 metros en Kelvin.
# Conversion vectorizada: Celsius = Kelvin - 273.15
t_celsius = ds["t2m"] - 273.15
t_celsius.name = "temperatura_celsius"
t_celsius.attrs["units"] = "degC"
t_celsius.attrs["long_name"] = "Temperatura a 2 m en Celsius"


# -------------------------------------------------------------------------
# 4. Remuestreo temporal: horario -> diario
# -------------------------------------------------------------------------
# El cubo esta en resolucion horaria. Para temperatura se calcula media diaria.
# Si fuera precipitacion, normalmente se usaria .sum().
t_diaria = t_celsius.resample(valid_time="1D").mean()
t_diaria.name = "temperatura_media_diaria"
t_diaria.attrs["units"] = "degC"
t_diaria.attrs["long_name"] = "Temperatura media diaria a 2 m"

print("\n=== CUBO DIARIO ===")
print(t_diaria)


# -------------------------------------------------------------------------
# 5. Maxima mensual y fecha de ocurrencia
# -------------------------------------------------------------------------
# Maximo mensual por pixel:
# - Para cada mes y cada celda espacial, se obtiene el mayor valor diario.
# Fecha de ocurrencia:
# - idxmax devuelve la fecha exacta en la que ocurre el maximo diario.
# - Se fuerza un solo chunk temporal para que idxmax trabaje bien con dask.
t_diaria = t_diaria.chunk({"valid_time": -1})

t_max_mensual = t_diaria.resample(valid_time="MS").max()
t_max_mensual.name = "temperatura_maxima_mensual"
t_max_mensual.attrs = {
    "units": "degC",
    "long_name": "Maxima mensual de temperatura media diaria",
}

fecha_max_mensual = t_diaria.resample(valid_time="MS").map(
    lambda bloque_mensual: bloque_mensual.idxmax(dim="valid_time")
)
fecha_max_mensual.name = "fecha_maxima_mensual"
fecha_max_mensual.attrs = {
    "long_name": "Fecha de ocurrencia de la maxima mensual",
}


# -------------------------------------------------------------------------
# 6. Cubo final y persistencia en NetCDF
# -------------------------------------------------------------------------
# Se combinan las variables finales en un solo Dataset.
resultado = xr.Dataset(
    {
        "temperatura_maxima_mensual": t_max_mensual,
        "fecha_maxima_mensual": fecha_max_mensual,
    }
)

resultado = resultado.rio.set_spatial_dims(
    x_dim="longitude", y_dim="latitude", inplace=False
)
resultado = resultado.rio.write_crs("EPSG:4326", inplace=False)

print("\n=== RESULTADO FINAL ===")
print(resultado)

# Compresion ligera para reducir tamano del archivo final.
encoding = {
    "temperatura_maxima_mensual": {"zlib": True, "complevel": 4},
    "fecha_maxima_mensual": {"zlib": True, "complevel": 4},
}

resultado.to_netcdf(output_file, encoding=encoding)

print(f"\nArchivo exportado en: {output_file}")
