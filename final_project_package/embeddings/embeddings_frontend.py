from torch import no_grad, tensor

# Get Text Embeddings
def get_text_embeddings(model, processor, texts):
    print("Generating text embeddings for labels...")
    inputs = processor(text=texts, return_tensors="pt", padding=True)
    with no_grad():
        text_features = model.get_text_features(**inputs)[1][0]
    text_features = text_features / text_features.norm(p=2, dim=-1, keepdim=True)
    return text_features

def similarity(image_embedding, text_embedding):
    # Compute cosine similarity: (n_clusters, 512) @ (512, n_labels) -> (n_clusters, n_labels)
    similarity = (image_embedding @ tensor(text_embedding).T).numpy()

    return similarity
