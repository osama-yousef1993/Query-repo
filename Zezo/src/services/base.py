from PIL import Image

from configs.configuration import API_BASE_URL, API_KEY


class Base_Operations:
    def __init__(self, image_path, endpoint=str, public_id=str):
        self.api_base_url = API_BASE_URL
        self.image_path = image_path
        self.api_key = API_KEY

    def read_image(image_path) -> Image:
        """
        Reads an image from the specified path and returns the image object.
        Parameters:image_path (str): The path to the image file.
        Returns: PIL.Image.Image: The image object.
        """
        try:
            img = Image.open(image_path)
            return img
        except IOError:
            print(
                f"Error opening the image file at {image_path}. Please check the file path and try again."
            )
            return None

    def copy_image(img, copy_path) -> None:
        """
        Creates a copy of the given image object and saves it to the specified path.
        Parameters:
            img (PIL.Image.Image): The image object to be copied.
            copy_path (str): The path where the copy of the image will be saved.
        """
        if img is not None:
            img.save(copy_path)
            print(f"Image successfully copied to {copy_path}.")
        else:
            print("No image to copy.")
