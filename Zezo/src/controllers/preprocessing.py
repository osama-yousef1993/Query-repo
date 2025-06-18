from io import BytesIO

from fastapi import APIRouter, File, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse


from src.services.preprocessing import Preprocessing_Operations

PreprocessingRouter = APIRouter()


@PreprocessingRouter.post("/enhance-photo/")
async def enhance_photo(image: UploadFile = File(...)):
    endpoint = "enhance-photo"
    # Validate image format (for this example, we're only allowing JPEGs)
    if image.content_type not in ["image/jpeg", "image/png"]:
        content = {"detail": f"Error in {endpoint} endpoint."}
        return JSONResponse(content=content, status_code=status.HTTP_409_CONFLICT)
        # raise HTTPException(status_code=400, detail="Unsupported image format. Please upload a JPEG or PNG image.")

    try:
        # Read the image data
        image_data = await image.read()

        # Apply the photo enhancer function
        operation_object = Preprocessing_Operations(endpoint)
        enhanced_image_data = operation_object.photo_enhancer(image_data)

        # Return the enhanced image
        return StreamingResponse(
            BytesIO(enhanced_image_data), media_type=image.content_type
        )
    except Exception:
        content = {"detail": f"Error in {endpoint} endpoint."}
        raise JSONResponse(content=content, status_code=status.HTTP_409_CONFLICT)
