# #| eval: false

"""
Verificación integral de un cubo NetCDF ERA5 (EDA)

Este script:
1. Controla existencia de archivos (.nc / .zip)
2. Carga el dataset en modo lazy
3. Inspecciona:
   - dimensiones
   - variable principal
   - metadatos
   - forma del arreglo
4. Verifica la estructura vertical (niveles de presión)

Contexto físico:
---------------
La presión disminuye con la altura:
    ~1 hPa     → estratosfera alta
    ~500 hPa   → troposfera media
    ~1000 hPa  → superficie
"""

import os
import sysconfig
import zipfile

proj_data_dir = os.path.join(sysconfig.get_paths()["purelib"], "rasterio", "proj_data")
os.environ["PROJ_LIB"] = proj_data_dir
os.environ["PROJ_DATA"] = proj_data_dir

import xarray as xr
import rioxarray

# -------------------------------------------------------------------------
# Rutas
# -------------------------------------------------------------------------

path = "./data_heavy/"
ncname = "download5Dcolombia"
ncfile5d = os.path.join(path, ncname + ".nc")
zipfile_path = os.path.join(path, ncname + ".zip")


# -------------------------------------------------------------------------
# Control de existencia y descompresión
# -------------------------------------------------------------------------

if not os.path.exists(ncfile5d):

    if os.path.exists(zipfile_path):
        print(f"Archivo .nc no encontrado. Descomprimiendo {zipfile_path}")
        with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
            zip_ref.extractall(path)
    else:
        raise FileNotFoundError("No existe .nc ni .zip")

else:
    print(f"Usando archivo existente: {ncfile5d}")

# -------------------------------------------------------------------------
# Carga (lazy)
# -------------------------------------------------------------------------

# Carga lazy con Dask
ds = xr.open_dataset(ncfile5d, chunks="auto")

# chunks="auto" activa Dask y divide el cubo en bloques manejables para la RAM
# Se requieren las librerías:
# xarray (instalada en el contenedor),
# netcdf4 (no instalada en el contenedor)
# y toolz (no instalada en el contenedor)
# Si no quieres usar Dask, puedes abrir el archivo así:
# ds = xr.open_dataset(ncfile5d)

print("\n=== DATASET ===")
print(ds)

# -------------------------------------------------------------------------
# Dimensiones
# -------------------------------------------------------------------------

print("\n=== DIMENSIONES ===")
print(ds.dims)


# -------------------------------------------------------------------------
# Variable principal
# -------------------------------------------------------------------------

print("\n=== VARIABLES ===")
print(list(ds.data_vars))

# -------------------------------------------------------------------------
# Metadatos
# -------------------------------------------------------------------------

print("\n=== METADATOS ===")
print(ds.attrs)

# -------------------------------------------------------------------------
# Forma del arreglo
# -------------------------------------------------------------------------

print("\n=== SHAPE ===")
print(ds["t"].shape)


# -------------------------------------------------------------------------
# Niveles de presión
# -------------------------------------------------------------------------

z_levels = ds.level.values

print("\nPrimeros niveles (Alta atmósfera):")
print(z_levels[:2])

print("\nÚltimos niveles (Superficie):")
print(z_levels[-2:])



# #| eval: false

import matplotlib.pyplot as plt

# ============================================================
# Selección avanzada en cubo 5D (ERA5) con xarray
# - Selección por dimensiones (level, number, time)
# - Selección espacial (bounding box Bogotá)
# - Extracción de variable
# - Reducción a slice 2D + Visualización
# ============================================================

# -------------------------------------------------------------------------
# Selección 1: Subconjunto por dimensiones (label-based)
# -------------------------------------------------------------------------
# Selecciona:
# - miembro de ensamble 0
# - primeros dos niveles de presión
# - primer instante temporal
sel_dim = ds.sel(
    number=0,
    level=ds.level[:2],
    time=ds.time[0]
)

print(sel_dim)

# -------------------------------------------------------------------------
# Selección 2: Recorte espacial (Bogotá - WGS84)
# -------------------------------------------------------------------------
# Nota:
# - longitude = eje X
# - latitude  = eje Y
# - slice respeta orden de coordenadas

sel_bogota = ds.sel(
    longitude=slice(-74.25, -73.90),
    latitude=slice(4.85, 4.45)
)

print(sel_bogota)

# -------------------------------------------------------------------------
# Extracción de variable
# -------------------------------------------------------------------------
# 't' corresponde a temperatura (K)
t_bogota = sel_bogota["t"]

print(t_bogota)

# -------------------------------------------------------------------------
# Slice 2D (reducción dimensional)
# -------------------------------------------------------------------------
# Selecciona:
# - nivel más bajo (aprox. superficie)
# - ensamble 0
# - tiempo 0

capa_2d = t_bogota.isel(
    level=-1,
    number=0,
    time=0
)

print(capa_2d)

# Visualización de la matriz de datos
# El objeto se materializa (RAM) al usar .values
print(capa_2d.values)

# Verificación de las dimensiones restantes
print(capa_2d.dims)

# Verificación de los valores de las coordenadas
print(capa_2d.coords)

# Extracción de los valores de latitud o longitud asociados
eje_x = capa_2d.longitude.values
eje_y = capa_2d.latitude.values
print(eje_x)
print(eje_y)

# Verificación y extracción robusta
for dim in capa_2d.dims:
    print(f"Valores en la dimensión {dim}: {capa_2d[dim].values}")

# -------------------------------------------------------------------------
# Plot
# -------------------------------------------------------------------------

#if capa_2d.size > 1:
    # Imprimir como mapa: requiere mínimo matriz de 2 x 2
    # El objeto se materializa (RAM) al usar .plot()
    #capa_2d.plot(cmap="coolwarm")
#else:
#    print("⚠️ Solo hay un pixel, no se puede hacer plot espacial")
    # Esto ya no es un mapa, es un escalar
    #plt.imshow(capa_2d.values.reshape(1,1), cmap="coolwarm")
    #plt.colorbar(label="Temperatura (K)")
    #plt.title("Temperatura ERA5 - Bogotá (Superficie)")
    #plt.show()

# #| eval: false

# 1. Calcular el promedio de temperatura sobre los 10 miembros de ensamble
# Esto reduce la dimensión - number - y nos deja con la 'media del ensamble'
# Nota: Al usar dask el objeto t_mean no se calcula inmediatamente
#       Si usas ds = xr.open_dataset(ncfile5d), t_mean va a RAM!
t_mean = ds['t'].mean(dim='number')
t_mean
print(t_mean)

# 2. Seleccionar un nivel de presión específico para Bogotá (aprox 750 hPa)
# Buscamos el nivel más cercano a la presión de la capital
t_bogota_alt = t_mean.sel(level=750, method='nearest')
t_bogota_alt
print(t_bogota_alt)

# #| eval: false

# Resta vectorizada a todo el cubo de medias
t_celsius = t_mean - 273.15

# Actualización de atributos
t_celsius.attrs['units'] = '°C'
t_celsius.attrs['long_name'] = 'Temperatura Media (Celsius)'

# Verificación en el nivel de Bogotá
t_bogota_celsius = t_celsius.sel(level=750, method='nearest')
print(t_bogota_celsius)

import matplotlib.pyplot as plt
# Visualización de la rebanada (Tiempo 0)
t_bogota_celsius.isel(time=0).plot(cmap="inferno")
plt.title("Temperatura ERA5 Bogotá - Nivel 750 hPa (°C)")
plt.show()


# #| eval: false

# -------------------------------------------------------------------------
# Inspección de resolución temporal
# -------------------------------------------------------------------------
import pandas as pd

# Calculamos la diferencia entre los dos primeros registros de tiempo
delta_t = ds.time.diff(dim="time")[0].values
# Convertimos a horas (float)
horas_paso = delta_t / pd.Timedelta('1 hour')

print(f"Resolución temporal detectada: {horas_paso} horas")


# -------------------------------------------------------------------------
# Remuestreo temporal con xarray (Resampling)
# -------------------------------------------------------------------------

# Partimos del cubo t_celsius (que ya tiene el ensamble promediado)
# .resample() agrupa la dimensión de tiempo. "1D" significa "1 Día" (Diario).
# Luego aplicamos .mean() para calcular la temperatura media diaria.
t_diaria = t_celsius.resample(time="1D").mean()

print("\n--- Estructura Original ---")
print(t_celsius.time)

print("\n--- Estructura Remuestreada (Diaria) ---")
print(t_diaria.time)

# Seleccionamos: Nivel 750hPa Y un punto específico de Lat/Lon
# (Cerca del centro de Bogotá)
punto_especifico = t_diaria.sel(
    level=750, 
    latitude=4.6, 
    longitude=-74.0, 
    method='nearest'
)

# Ahora sí, graficar una línea
plt.figure(figsize=(10, 4))
punto_especifico.plot(marker='o', color='firebrick')
plt.title("Evolución de la Temperatura Diaria en un punto de Bogotá")
plt.ylabel("Temperatura Media [°C]")
plt.grid(True)
plt.show()

# #| eval: false

# -------------------------------------------------------------------------
# Reproyección espacial de cubos multidimensionales (Warping)
# -------------------------------------------------------------------------

# 1. Selección de nivel y re-asignación de CRS (Seguridad)
t_3d = t_diaria.sel(level=750, method='nearest')
t_3d.rio.write_crs("epsg:4326", inplace=True)

# 2. Transformación a MAGNA-SIRGAS Origen Nacional (Metros)
# Se aplica el algoritmo bilineal para variables continuas.
t_proyectado = t_3d.rio.reproject("EPSG:9377", resampling=1)

# 3. Validación de la estructura resultante
print(f"Estructura 3D proyectada: {t_proyectado.shape}")
print(f"Nuevo CRS: {t_proyectado.rio.crs}")

# 4. Renderizado de validación métrica
plt.figure(figsize=(8, 6))
t_proyectado.isel(time=0).plot(cmap="magma")
plt.title("Temperatura Diaria Bogotá (750 hPa)\n(Unidades en Metros)")
plt.grid(True, alpha=0.3)
plt.show()

