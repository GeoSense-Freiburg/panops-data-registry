"""Simple Google Earth Engine authentication script"""
from typing import Optional

import click
import ee


def init_ee_session(
    service_account: str, private_key: str, project: Optional[str] = None
) -> None:
    """Initialize the Earth Engine session."""
    ee.Authenticate()
    ee.Initialize(
        {"service_account": service_account, "private_key": private_key},
        project=project,
    )


@click.command()
@click.argument("project", type=str, default=None, required=False)
def main(project: str) -> None:
    """Main function."""
    init_ee_session(project=project)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
