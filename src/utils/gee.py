"""Google Earth Engine utility functions."""

from typing import Optional

import ee


def get_ic(
    product: str, date_start: str, date_end: str, bands: Optional[list] = None
) -> ee.ImageCollection:
    """
    Get an ee.ImageCollection.

    Args:
        product (str): The name of the product.
        bands (list): A list of band names.
        date_start (str): The start date in 'YYYY-MM-DD' format.
        date_end (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        ee.ImageCollection: The filtered ee.ImageCollection.
    """
    ic = ee.ImageCollection(product).filterDate(date_start, date_end)
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
