from torch import no_grad
from transformers import CLIPProcessor, CLIPModel

# Setup and Loading Model
def load_clip_model():
    print("Loading CLIP model...")
    model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
    processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
    return model, processor

# Get image embedding
def get_image_embeddings(model, processor, images):
    try:
        inputs = processor(images=images, return_tensors="pt", padding=True)
        with no_grad():
            image_features = model.get_image_features(**inputs)[1][0]
        # Normalize embeddings
        image_features = image_features / image_features.norm(p=2, dim=-1, keepdim=True)
        embedding = image_features.numpy().tolist()
    except:
        embedding = []

    return embedding
