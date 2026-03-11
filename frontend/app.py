import streamlit as st
import pandas as pd
from pathlib import Path
import requests
from final_project_package.embeddings.embeddings import load_clip_model
from final_project_package.embeddings.embeddings_frontend import get_text_embeddings, similarity

st.state.model, st.state.processor = load_clip_model()
st.state.image_df = pd.read_csv(Path("../data_dump/images_cleaned_embedding.csv"))

'''
# UrbanScore
'''

st.markdown('''
Remember that there are several ways to output content into your web page...

Either as with the title by just creating a string (or an f-string). Or as with this paragraph using the `st.` functions
''')

'''
## Please enter the parameters of the search
'''

query = st.text_input("Image Query", value="skandenavian kitchen")

variable = st.text_input("Variable", value="Square Meters")

min_value = st.number_input("Minimum value of Variable", min_value=0, max_value=100000000, value=0, step=0.1)

text_embedding = get_text_embeddings(st.state.model, st.state.processor, [query])

similarity = 0.1

center_lon = 35.4123
center_lat = 139.4132

df = pd.DataFrame({
    "lon": [center_lon-0.01, center_lon+0.01],
    "lat": [center_lat-0.01, center_lat+0.01]
})

st.map(df)


'''
## Predicted fare for this ride

'''

#@st.cache_data


st.write("Similarity: ", similarity)
