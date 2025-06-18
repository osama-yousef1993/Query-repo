import requests

from configs.configuration import API_BASE_URL, API_KEY


class Preprocessing_Operations:
    def __init__(self, image_path):
        self.api_base_url = API_BASE_URL
        self.image_path = image_path
        self.api_key = API_KEY

    def photo_enhancer(self, img):
        """
        Enhances the quality of photos that have issues such as bad focus, low resolution, blurriness, pixelation, or damage.
        Transforms every photo into a high-definition version with sharp focus.
        Raises:
            Exception: If the request to the enhancement API fails.
        """
        try:
            endpoint = "/api/v1/photoEnhance"
            response = requests.post(
                self.api_base_url + endpoint,
                files={"file": img},
                headers={"APIKEY": self.api_key},
            )
            response.raise_for_status()  # This will raise an exception for HTTP error codes
            return response.content
        except requests.exceptions.RequestException as e:
            raise Exception(f"An error occurred while enhancing the photo: {e}")

    def remove_background(self, img):
        """
        Automatically recognizes the foreground in the image, separates it from the background,
        and returns a transparent image (foreground colors along with the alpha matte).
        This function works for images with a distinguishable foreground
        (e.g., people, animals, products, cartoons).

        Parameters:
        img (bytes or file-like object): The image data or a file-like object containing the image.
        Returns:
            bytes: The image data with the background removed.
        Raises:
            Exception: If there's an error with the API request or if the input is not in the expected format.
        """
        try:
            endpoint = "/api/v1/matting?mattingType=6"
            response = requests.post(
                self.api_base_url + endpoint,
                files={"file": img},
                headers={"APIKEY": self.api_key},
            )
            response.raise_for_status()  # Checks for HTTP errors and raises an exception if found
            return response.content
        except requests.exceptions.RequestException as e:
            # Rethrow with a more descriptive error message
            raise Exception(f"An error occurred while removing the background: {e}")

    def photo_colorizer(self, img):
        """
        Colorizes black and white photos using advanced AI algorithms. This function is ideal for
        enhancing old family photos, historical figure portraits, and any black and white images that
        could benefit from realistic coloring. It provides a way to restore and refresh memories with
        stunning colors through a few clicks.
        Parameters:
            img (bytes or file-like object): The black and white image data or a file-like object containing the image to be colorized.
        Returns:
            bytes: The colorized image data.
        Raises:
            ValueError: If 'img' is neither bytes nor a file-like object.
            Exception: If there's an error with the API request.
        """
        try:
            endpoint = "/api/v1/matting?mattingType=19"
            response = requests.post(
                self.api_base_url + endpoint,
                files={"file": img},
                headers={"APIKEY": self.api_key},
            )
            response.raise_for_status()  # This will raise an exception for HTTP error codes
            return response.content
        except requests.exceptions.RequestException as e:
            raise Exception(f"An error occurred while colorizing the photo: {e}")
