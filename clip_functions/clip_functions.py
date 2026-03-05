import torch
from transformers import pipeline

def initialize_clip():
    clip = pipeline(
        task = "zero-shot-image-classification",
        model = "openai/clip-vit-base-patch32",
        dtype = torch.bfloat16,
        device=0
    )
    return clip


def identify_default_images(image_path: str, clip):
    """
    Use CLIP to identify default images.
    Default images are computer generated images.
    Return 1 or 0.

    Input: image path
    Output: 1 if DefaultImage, 0 if not DefaultImage
    """

    try:
        labels = ["illustration, logo or advert", "floor plan", "image"]
        results = clip(image_path, candidate_labels=labels)

        if results[0]["label"] == labels[0]:
            default_image = 2
        elif results[0]["label"] == labels[1]:
            default_image = 1
        else:
            default_image = 0

    except:
        default_image = 2

    return default_image


def assign_room_type(image_path: str, labels: list, clip):
    """
    Use CLIP to identify the room type of the image.
    Results are list of dictionaries, automatically sorted by decreasing score.
    Return label ob first entry in results.

    Input: image path and list of possible room types
    Output: label of room type depicted in the image
    """
    results = clip(image_path, candidate_labels=labels)
    room_type = results[0]["label"]
    score = results[0]["score"]

    return room_type


def get_score(image_path: str, room_type: str, attribute_list: list, clip):
    """
    Use CLIP to score the room according to each attribute.
    The list of attributes depends on the room type according to the attribute_dict.
    Returns dictionary with attributes as keys and scores as values.

    Input: image path and list of possible room types
    Output: label of room type depicted in the image
    """

    dict = {}

    for attribute in attribute_list:
        if room_type == "floor plan":
            dict[attribute] = -1000

        else:
            if attribute == "brightness":
                labels = ["bright", "dark"]
            elif attribute == "luxury":
                labels = ["expensive", "cheap"]
            elif attribute == "modernity":
                labels = ["modern", "old fashioned"]
            else:
                labels = ["yes "+attribute, "no "+attribute]

            results = clip(image_path, candidate_labels=labels)
            label = results[0]["label"]
            score = results[0]["score"]

            if label != labels[0]:
                score = 1-score

            dict[attribute] = score

    return dict
