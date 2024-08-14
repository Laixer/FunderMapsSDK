import sys
import logging
import asyncio

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig
from fundermapssdk.tippecanoe import tippecanoe


logger = logging.getLogger("map_gen")


async def run(config):
    db_config = DatabaseConfig(
        database=config.get("database", "database"),
        host=config.get("database", "host"),
        user=config.get("database", "username"),
        password=config.get("database", "password"),
        port=config.getint("database", "port"),
    )

    fundermaps = FunderMapsSDK(db_config=db_config)

    logger.info("Starting BAG loading process")

    # if (bundle.MapEnabled)
    # {
    #     logger.LogInformation("Generating map for tileset '{Tileset}'", bundle.Tileset);

    #     gdalService.Convert($"{bundle.Tileset}.gpkg", $"{bundle.Tileset}.geojson");
    #     tilesetGeneratorService.Generate($"{bundle.Tileset}.geojson", $"{bundle.Tileset}.mbtiles", bundle.Tileset, bundle.MaxZoomLevel, bundle.MinZoomLevel, cancellationToken);

    #     logger.LogInformation("Uploading tileset to mapbox '{Tileset}'", bundle.Tileset);

    #     await mapboxService.UploadAsync(bundle.Name, bundle.Tileset, $"{bundle.Tileset}.mbtiles");
    # }

    # logger.LogInformation("Storing tileset '{Tileset}'", bundle.Tileset);

    # DateTime currentDate = DateTime.Now;
    # string dateString = currentDate.ToString("yyyy-MM-dd");
    # await blobStorageService.StoreFileAsync($"tileset/archive/{dateString}/{bundle.Tileset}.gpkg", $"{bundle.Tileset}.gpkg");
    # await blobStorageService.StoreFileAsync($"tileset/{bundle.Tileset}.gpkg", $"{bundle.Tileset}.gpkg");

    # logger.info("Loading BAG file into database")
    await fundermaps.gdal.convert(
        "PG:dbname=fundermaps",
        "facade_scan.gpkg",
        "maplayer.facade_scan",
    )
    await fundermaps.gdal.convert(
        "facade_scan.gpkg",
        "facade_scan.geojson",
    )

    await tippecanoe(
        "facade_scan.geojson", "facade_scan.mbtiles", "facade_scan", 16, 12
    )

    # TODO: Upload to Mapbox
    # TODO: Upload to DigitalOcean Spaces


def main():
    import colorlog
    from configparser import ConfigParser

    config = ConfigParser()

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
    )

    config.read("config.ini")

    try:
        logger.info("Starting 'loadbag'")
        asyncio.run(run(config))
        logger.info("Finished 'loadbag'")
    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)


if __name__ == "__main__":
    main()
