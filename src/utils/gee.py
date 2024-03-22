"""Google Earth Engine utility functions."""

# pylint: disable=no-member

from dataclasses import dataclass
from typing import Optional

import ee


def get_ic(
    product: str,
    date_start: str,
    date_end: str,
    bands: Optional[list] = None,
    bounds: Optional[ee.Geometry] = None,
) -> ee.ImageCollection:
    """
    Get an ee.ImageCollection.

    Args:
        product (str): The name of the product.
        date_start (str): The start date in 'YYYY-MM-DD' format.
        date_end (str): The end date in 'YYYY-MM-DD' format.
        bands (list, optional): A list of band names to select. Defaults to None.
        bounds (ee.Geometry, optional): The bounds to filter the image collection. Defaults to None.

    Returns:
        ee.ImageCollection: The filtered ee.ImageCollection.
    """
    ic = ee.ImageCollection(product).filterDate(date_start, date_end)

    if bounds is not None:
        ic = ic.filterBounds(bounds)

    if bands is not None:
        return ic.select(bands)
    return ic


def bitwise_extract(
    image: ee.Image, from_bit: int, to_bit: Optional[int] = None
) -> ee.Image:
    """Performs bitwise extraction for masking images from QA bands

    Args:
        image (ee.Image): Single-band image (QA Band) with n-bit values
        from_bit (int): Position of starting bit
        to_bit (int, optional): Position of ending bit (if values span multiple bits).
        Defaults to None.

    Returns:
        ee.Image
    """
    if to_bit is None:
        to_bit = from_bit
    mask_size = ee.Number(1).add(to_bit).subtract(from_bit)
    mask = ee.Number(1).leftShift(mask_size).subtract(1)
    return image.rightShift(from_bit).bitwiseAnd(mask)


def mask_clouds(
    ic: ee.ImageCollection, qa_band: str = "state_1km"
) -> ee.ImageCollection:
    """Masks cloudy (or cloud-shadowed) pixels of MODIS Terra Land Reflectance images
    contained within ImageCollection

    Args:
        ic (ee.ImageCollection): ImageCollection to be masked
        qa_band (str): QA band name

    Returns:
        ee.ImageCollection: Masked image collection
    """

    def _mask_clouds(image: ee.Image) -> ee.Image:
        qa = image.select(qa_band)
        cloud_mask = bitwise_extract(qa, 0, 1).eq(0).Or(bitwise_extract(qa, 0, 1).eq(3))
        internal_cloud_mask = bitwise_extract(qa, 10).eq(0)
        mask = internal_cloud_mask.And(cloud_mask)
        image_masked = image.updateMask(mask)

        return image_masked

    return ic.map(_mask_clouds)


def calculate_monthly_averages(ic: ee.ImageCollection, year_start: str, year_end: str):
    """
    Calculate monthly averages for each band in an ImageCollection.

    Parameters:
        ic (ee.ImageCollection): The input image collection.
        year_start (str): The starting year for calculating monthly averages.
        year_end (str): The ending year for calculating monthly averages.

    Returns:
        ee.ImageCollection: The image collection containing monthly average images for each band.
    """

    months = range(1, 13)

    # Get the first image in the collection to retrieve band names
    first_image = ee.Image(ic.first())
    bands = first_image.bandNames().getInfo()

    monthly_averages = []

    for month in months:
        for band in bands:
            bn = f"{band}_{year_start}-{year_end}_m{month}_mean"
            mean_image = (
                ic.filter(ee.Filter.calendarRange(month, month, "month"))
                .select(band)
                .mean()
                .rename(bn)
            )
            monthly_averages.append(mean_image)

    return ee.ImageCollection(monthly_averages)


@dataclass
class ExportParams:
    """
    Represents the parameters for exporting data in Google Earth Engine.

    Attributes:
        crs (str): The coordinate reference system (CRS) for the exported data. Default is "EPSG:4326".
        scale (int): The scale of the exported data. Default is 1000.
        target (str): The target destination for the exported data. Default is "gcs".
        folder (str): The folder name for storing the exported data. Default is "gee_exports".
    """

    crs: str = "EPSG:4326"
    scale: int = 1000
    target: str = "gcs"
    folder: str = "gee_exports"

    def __post_init__(self):
        if self.target not in ["gcs", "gdrive"]:
            raise ValueError("Invalid target. Use 'gcs' or 'gdrive'.")


def export_image(image: ee.Image, filename: str, export_params: ExportParams) -> None:
    """Export an image to Drive or Google Cloud Storage.

    Args:
        image (ee.Image): Image to export.
        filename (str): Filename to be used as prefix of exported file and name of task.
        export_params (ExportParams): Export parameters including destination folder, projection, and scale.

    Returns:
        None
    """
    task_config = {
        "description": filename,
        "fileNamePrefix": filename,
        "crs": export_params.crs,
        "fileFormat": "GeoTIFF",
        "formatOptions": {"cloudOptimized": True, "noData": -32768},
        "maxPixels": 1e13,
        "scale": export_params.scale,
        "skipEmptyTiles": True,
    }

    if export_params.target == "gcs":
        task_config["bucket"] = export_params.folder
        task = ee.batch.Export.image.toCloudStorage(image, **task_config)
        task.start()

    else:
        task_config["folder"] = export_params.folder
        task = ee.batch.Export.image.toDrive(image, **task_config)
        task.start()


def export_collection(
    collection: ee.ImageCollection,
    export_params: ExportParams,
    test: bool = False,
) -> None:
    """Export an ImageCollection to Drive

    Args:
        collection (ee.ImageCollection): ImageCollection to be exported
        export_params (ExportParams): Export parameters specifying the destination folder, projection, and scale
        test (bool, optional): If True, only exports the first image in the collection for testing purposes.
            Defaults to False.

    Returns:
        None
    """
    num_images = int(collection.size().getInfo())
    image_list = collection.toList(num_images)

    for i in range(num_images if not test else 1):
        image = ee.Image(image_list.get(i))
        out_name = f"{image.bandNames().getInfo()[0]}"
        print(f"Exporting {out_name} with scale {export_params.scale} m")
        export_image(image, out_name, export_params)
