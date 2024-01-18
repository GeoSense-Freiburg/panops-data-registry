import io
import logging
import time

import ee
import hydra

from data.ee_auth import init_ee_session

# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload


log = logging.getLogger(__name__)


@hydra.main(version_base=None, config_path="../../conf", config_name="config")
def main(cfg) -> None:
    # Initialize Earth Engine
    init_ee_session(project=cfg.gee.land_mask_project)

    # # Access the ESA WorldCover 10m v200 dataset.
    # dataset = ee.ImageCollection("ESA/WorldCover/v200")

    # # Select only bands 10, 20, 30, 90, 95, and 100.
    # # 10  	Tree cover
    # # 20  	Shrubland
    # # 30  	Grassland
    # # 40  	Cropland
    # # 50  	Built-up
    # # 60  	Bare / sparse vegetation
    # # 70  	Snow and ice
    # # 80  	Permanent water bodies
    # # 90  	Herbaceous wetland
    # # 95  	Mangroves
    # # 100  	Moss and lichen
    # selected_bands = dataset.select(["10", "20", "30", "90", "95", "100"])

    # # Resample the dataset to 500m resolution.
    # resampled_dataset = selected_bands.reduceResolution(
    #     reducer=ee.Reducer.mean(), maxPixels=1024
    # ).reproject(crs=selected_bands.first().projection(), scale=500)

    # # Save the dataset to a Google Drive folder called "land_cover".
    # task = ee.batch.Export.image.toDrive(
    #     image=resampled_dataset,
    #     description="land_cover",
    #     folder="land_cover",
    #     scale=500,
    # )
    # task.start()

    # # Wait for the task to complete.
    # while task.active():
    #     log.info(f"Polling for task (id: {task.id}).")
    #     time.sleep(5)

    # # Download the dataset from Google Drive to the path data/raw/land_mask using DVC.
    # drive_service = build("drive", "v3")
    # results = (
    #     drive_service.files()
    #     .list(q="name = 'land_cover'", fields="files(id)")
    #     .execute()
    # )
    # file_id = results.get("files", [])[0]["id"]
    # request = drive_service.files().get_media(fileId=file_id)
    # fh = io.FileIO("data/raw/land_mask", "wb")
    # downloader = MediaIoBaseDownload(fh, request)
    # DONE = False
    # while DONE is False:
    #     status, DONE = downloader.next_chunk()
    #     log.info(f"Download {int(status.progress() * 100):d}.")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
