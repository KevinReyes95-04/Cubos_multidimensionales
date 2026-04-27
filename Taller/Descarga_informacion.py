import os
import socket
import cdsapi

for proxy_var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(proxy_var, None)

dns_fallback = {
    "cds.climate.copernicus.eu": "136.156.139.54",
    "object-store.os-api.cci2.ecmwf.int": "136.156.136.3",
}

original_getaddrinfo = socket.getaddrinfo


def getaddrinfo(host, port, *args, **kwargs):
    try:
        return original_getaddrinfo(host, port, *args, **kwargs)
    except socket.gaierror:
        if host in dns_fallback:
            return original_getaddrinfo(dns_fallback[host], port, *args, **kwargs)
        raise


socket.getaddrinfo = getaddrinfo


data_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data_heavy")
)
os.makedirs(data_dir, exist_ok=True)

output_file = os.path.join(data_dir, "era5_2m_temperature_2025_04_colombia.nc")

dataset = "reanalysis-era5-single-levels"
request = {
    "product_type": ["reanalysis"],
    "variable": ["2m_temperature"],
    "year": ["2025"],
    "month": ["04"],
    "day": [f"{day:02d}" for day in range(1, 31)],
    "time": [f"{hour:02d}:00" for hour in range(24)],
    "data_format": "netcdf",
    "download_format": "unarchived",
    "area": [4, -75, 3, -74],
}

client = cdsapi.Client(retry_max=3, sleep_max=10)
client.retrieve(dataset, request).download(output_file)

print(f"Archivo descargado en: {output_file}")
