import streamlit as st
import pandas as pd
from pathlib import Path
from final_project_package.embeddings.embeddings import load_clip_model, get_text_embeddings, get_similarity
st.set_page_config(layout="wide")

@st.cache_data
def get_clip():
    return load_clip_model()

def get_images():
    df = pd.read_csv(Path.cwd() / "data_dump/images_cleaned_embedding.csv", nrows=1000)
    return df[df["embedding"] != "[]"]

@st.cache_data
def get_listings():
    df = pd.read_csv(Path.cwd() / "data_dump/listings_with_scores.csv")
    images_df = get_images()
    source_id = images_df["source_id"]
    df = df[df["source_id"].isin(source_id)]

    # add first images
    img = images_df[images_df["room_type"] != "floor plan"]
    first_images = img.drop_duplicates("source_id")
    df = df.merge(first_images, on="source_id", how="left")
    return df

model, processor = get_clip()
images_df = get_images()
listings_df = get_listings()


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

query = st.text_input("Image Query", value="")

variable = st.selectbox("Variable", list(listings_df.columns))

min_value = st.number_input("Minimum value of Variable", min_value=0.0, max_value=100000000.0, value=0.0, step=0.1)

text_embedding = get_text_embeddings(model, processor, [query])

similarity = images_df["embedding"].apply(lambda x: \
        get_similarity(x, text_embedding))


if query == "":
    listings = listings_df
else:
    source_id = images_df[similarity > 0.2]["source_id"]
    listings = listings_df[listings_df["source_id"].isin(source_id)]

df = pd.DataFrame({
    "lon": listings["longitude"],
    "lat": listings["latitude"]
})

st.map(df)


'''
## Listings

'''


# --- Card Styling ---
st.markdown("""
<style>
.card {
    border-radius: 12px;
    padding: 10px;
    margin-bottom: 20px;
    background-color: #f9f9f9;
    box-shadow: 0px 3px 8px rgba(0,0,0,0.1);
}
.card img {
    border-radius: 10px;
    height: 200px;
    overflow: hidden;
}
.price {
    font-size: 22px;
    font-weight: bold;
}
.meta {
    color: #555;
}
</style>
""", unsafe_allow_html=True)


# --- Display Listings ---
cols = st.columns(3)

for i, row in listings.iterrows():

    col = cols[i % 3]

    with col:
        st.markdown('<div class="card">', unsafe_allow_html=True)

        if pd.notna(row.get("image_url")):
            st.image(row["image_url"], use_container_width=True)

        st.markdown(
            f"""
            <div class="price">{row.get('price_man_yen','')} 万円</div>
            <div class="meta">{row.get('area_sqm','')} m²</div>
            <div>{row.get('address','')}</div>
            """,
            unsafe_allow_html=True
        )

        st.markdown('</div>', unsafe_allow_html=True)
