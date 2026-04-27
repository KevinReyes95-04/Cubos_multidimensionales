# #| eval: false

# ============================================================
# Lectura de NetCDF con múltiples bandas usando {xarray}
# + Decodificación automática de tiempo
# + Asignación de CRS WGS84 con {rioxarray}
# ============================================================

# -------------------------------
# 1. Cargar librerías necesarias
# -------------------------------
import urllib.request
import urllib.error
import zipfile
import os
import sysconfig
import xarray as xr
import pyproj
import matplotlib.pyplot as plt

proj_data_dir = os.path.join(sysconfig.get_paths()["purelib"], "rasterio", "proj_data")
if not os.path.exists(proj_data_dir):
    proj_data_dir = pyproj.datadir.get_data_dir()
os.environ["PROJ_LIB"] = proj_data_dir
os.environ["PROJ_DATA"] = proj_data_dir

import rioxarray

# -------------------------------
# 2. Configuración de rutas y directorios
# -------------------------------
# Definición del directorio de datos
data_dir = "./data_heavy/"

# Crear el directorio si no existe
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    print(f"Directorio creado: {data_dir}")

url = "https://geocorp.co/wind/goodland_10u_1.zip"
zip_file = os.path.join(data_dir, "10fg_2017_2018.zip")
nc_file = os.path.join(data_dir, "goodland_10u_1.nc")

# -------------------------------
# 3. Descargar y descomprimir datos
# -------------------------------
# Descargar archivo si no existe en la ruta especificada
if not os.path.exists(zip_file):
    print("Iniciando descarga...")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        urllib.request.install_opener(opener)
        urllib.request.urlretrieve(url, zip_file)
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"No se pudo descargar el archivo desde {url}. "
            "Revisa tu conexion a internet, DNS o configuracion de proxy."
        ) from error
    print(f"Archivo descargado en: {zip_file}")

# Descomprimir dentro de ./data_heavy/
with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    zip_ref.extractall(data_dir)
    print(f"Contenido extraído en: {data_dir}")

# -------------------------------
# 4. Leer el archivo NetCDF
# -------------------------------
# xarray lee las dimensiones y decodifica el tiempo desde la ruta local
if os.path.exists(nc_file):
    ds = xr.open_dataset(nc_file)
    print("Estructura del dataset:")
    print(ds)
else:
    print(f"Error: No se encontró el archivo {nc_file}")

# -------------------------------
# 5. Asignar sistema de referencia (CRS)
# -------------------------------
# xarray lee las dimensiones y decodifica el tiempo según convenciones CF
# Asignación de CRS WGS84 (EPSG:4326) usando rioxarray
ds.rio.write_crs("epsg:4326", inplace=True)

# -------------------------------
# 6. Ejemplo: extraer un tiempo específico
# -------------------------------
# Extraer la primera variable de datos (en este caso u10)
var_name = list(ds.data_vars)[0]
da = ds[var_name]

# Seleccionar el primer instante temporal (índice 0)
da_t0 = da.isel(time=0)
fecha_0 = da_t0["time"].dt.strftime("%Y-%m-%d %H:%M").item()

# Graficar
da_t0.plot(cmap="viridis")
# Rotar etiquetas de longitud para evitar solapamiento
plt.xticks(rotation=45)


# Ajustar el layout para que las etiquetas no se corten
plt.tight_layout()
plt.title(f"Velocidad del viento\n{fecha_0}")
plt.show()

# -------------------------------
# 7. Ejemplo: promedio temporal
# -------------------------------
# Calcular media a lo largo del tiempo
da_mean = da.mean(dim="time")

# Graficar
da_mean.plot(cmap="viridis")
# Rotar etiquetas de longitud para evitar solapamiento
plt.xticks(rotation=45)

# Ajustar el layout para que las etiquetas no se corten
plt.tight_layout()
plt.title("Velocidad promedio del viento")
plt.show()


