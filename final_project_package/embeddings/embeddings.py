from torch import no_grad, tensor
from transformers import CLIPProcessor, CLIPModel

# Setup and Loading Model
def load_clip_model():
    model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
    processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
    return model, processor

# Get Text Embeddings
def get_text_embeddings(model, processor, texts):
    inputs = processor(text=texts, return_tensors="pt", padding=True)
    with no_grad():
        text_features = model.get_text_features(**inputs)[1][0]
    text_features = text_features / text_features.norm(p=2, dim=-1, keepdim=True)
    return text_features

def get_similarity(image_embedding, text_embedding):
    # Compute cosine similarity: (n_clusters, 512) @ (512, n_labels) -> (n_clusters, n_labels)
    try:
        similarity = tensor(eval(image_embedding)) @ tensor(text_embedding.T).numpy()
    except:
        similarity = 0.0
    return similarity

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
