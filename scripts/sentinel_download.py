import requests
from sentinel_auth import get_access_token  # <- Make sure this is here
from config import BBOX

def download_sentinel_image(date_str: str, output_file: str):
    token = get_access_token()

    url = "https://services.sentinel-hub.com/api/v1/process"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    #B05, B06, B07 are the Red Edge bands for Sentinel-2
    #sample type is set to FLOAT32 for better precision
    evalscript = """
        //VERSION=3
        function setup() {
        return {
            input: ["B05", "B06", "B07"],
            output: {
            bands: 3,
            sampleType: "FLOAT32"
            }
        };
        }
        function evaluatePixel(sample) {
        return [sample.B05, sample.B06, sample.B07];
        }

    """

    payload = {
        "input": {
            "bounds": {
                "bbox": BBOX
            },
            "data": [{
                "type": "S2L1C",
                "dataFilter": {
                    "timeRange": {
                        "from": f"{date_str}T00:00:00Z",
                        "to": f"{date_str}T23:59:59Z"
                    }
                }
            }]
        },
        "evalscript": evalscript,
        "output": {
            "width": 512,
            "height": 512,
            "responses": [{
                "identifier": "default",
                "format": {
                    "type": "image/tiff"
                }
            }]
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        with open(output_file, "wb") as f:
            f.write(response.content)
        print(f"✅ Download complete: {output_file}")
    else:
        print(f"❌ Failed: {response.status_code}\n{response.text}")
