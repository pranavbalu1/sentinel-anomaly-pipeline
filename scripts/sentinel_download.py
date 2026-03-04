"""Sentinel Hub download client for SWIR bands used in methane anomaly detection."""

import requests

from config import BBOX, HEIGHT, WIDTH
from sentinel_auth import get_access_token


def download_sentinel_image(date_str: str, output_file: str) -> None:
    """Download B11/B12 TIFF imagery for a single UTC date into `output_file`."""
    token = get_access_token()

    url = "https://services.sentinel-hub.com/api/v1/process"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # B11/B12 are SWIR bands that are sensitive to methane absorption.
    # FLOAT32 preserves precision for downstream numerical processing.
    evalscript = """
        //VERSION=3
        function setup() {
        return {
            input: ["B11", "B12"],
            output: {
            bands: 2,
            sampleType: "FLOAT32"
            }
        };
        }
        function evaluatePixel(sample) {
        return [sample.B11, sample.B12];
        }
    """

    # Process API payload: define area, temporal window, and output TIFF format.
    payload = {
        "input": {
            "bounds": {"bbox": BBOX},
            "data": [{
                "type": "S2L1C",
                "dataFilter": {
                    "timeRange": {
                        "from": f"{date_str}T00:00:00Z",
                        "to": f"{date_str}T23:59:59Z",
                    }
                },
            }],
        },
        "evalscript": evalscript,
        "output": {
            "width": WIDTH,
            "height": HEIGHT,
            "responses": [{
                "identifier": "default",
                "format": {"type": "image/tiff"},
            }],
        },
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        # Persist raw binary TIFF locally for the Spark transform step.
        with open(output_file, "wb") as file_handle:
            file_handle.write(response.content)
        print(f"✅ Download complete: {output_file}")
        print(f"📅 Data Date: {date_str}")
        return

    print(f"❌ Failed: {response.status_code}\n{response.text}")
