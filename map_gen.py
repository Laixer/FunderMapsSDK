import os
import logging
import asyncio

from fundermapssdk import FunderMapsSDK
from fundermapssdk.mail import Email
from fundermapssdk.util import http_download_file
from fundermapssdk.config import MailConfig, DatabaseConfig
from fundermapssdk.gdal import convert
from fundermapssdk.tippecanoe import generate

BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"

logger = logging.getLogger("loadbag")


async def download_bag_file():
    """
    Download the BAG file from the PDOK service.
    """

    await http_download_file(BASE_URL_BAG, "bag-light.gpkg")


async def main(config):
    mail_config = MailConfig(
        api_key=config.get("mail", "api_key"),
        domain=config.get("mail", "domain"),
        default_sender_name=config.get("mail", "default_sender_name"),
        default_sender_address=config.get("mail", "default_sender_address"),
    )

    db_config = DatabaseConfig(
        database=config.get("database", "database"),
        host=config.get("database", "host"),
        user=config.get("database", "username"),
        password=config.get("database", "password"),
        port=config.getint("database", "port"),
    )

    fm = FunderMapsSDK(db_config=db_config, mail_config=mail_config)

    try:
        logger.info("Starting BAG loading process")

        # var input = $"PG:dbname='{dataSourceBuilder.Database}' host='{dataSourceBuilder.Host}' port='{dataSourceBuilder.Port}' user='{dataSourceBuilder.Username}' password='{dataSourceBuilder.Password}'";
        # gdalService.Convert(input, $"{bundle.Tileset}.gpkg", $"maplayer.{bundle.Tileset}");

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
        await convert(
            "PG:dbname=fundermaps host=db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com user=fundermaps password=AVNS_CtcfLEuVWqRXiK__gKt port=25060",
            "incident.gpkg",
            "maplayer.incident",
        )
        await convert("incident.gpkg", "incident.geojson")

        await generate("incident.geojson", "incident.mbtiles", "incident", 15, 10)

        # TODO: Upload to Mapbox

        # TODO: Upload to DigitalOcean Spaces

        logger.info("Finished")
    except Exception as e:

        # TODO: Cleanup db

        # to = config.get("general", "report_to")
        # await fm.mail.send_simple_message(
        #     Email(
        #         to=to.split(","),
        #         subject="Error loading BAG",
        #         text="An error occurred while loading the BAG file",
        #     )
        # )

        logger.error("An error occurred", exc_info=e)


if __name__ == "__main__":
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

    asyncio.run(main(config))
