import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
from final_project_package.embeddings.embeddings import load_clip_model, get_text_embeddings, get_similarity
st.set_page_config(layout="wide")

@st.cache_resource
def get_clip():
    return load_clip_model()

@st.cache_resource
def get_images():
    df = pd.read_csv(Path.cwd() / "data_dump/images_cleaned_embedding.csv", nrows=3000)
    return df[df["embedding"] != "[]"]

@st.cache_resource
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
### Search for Top 10 listings with specific properties in their images
'''

query = st.text_input("Image Query", value="")

'''
### Set additional filters
'''
col1, col2, col3 = st.columns(3)
with col1:
    variable = st.selectbox("Numeric variable", ["price_man_yen", "area_sqm", "year_built", "floor_number"])

with col2:
    min_value = st.number_input("Minimum value", min_value=0.0, max_value=100000000.0, step=0.1)

with col3:
    max_value = st.number_input("Maximum value", min_value=0.0, max_value=100000000.0, step=0.1)

st.markdown('''

''')


# Text embedding
text_embedding = get_text_embeddings(model, processor, [query])

similarity = images_df["embedding"].apply(lambda x: \
        get_similarity(x, text_embedding)).astype("float").to_frame()

if query == "":
    listings = listings_df
else:
    similarity["source_id"] = images_df["source_id"]
    similarity["image_url"] = images_df["image_url"]
    source_id = similarity.nlargest(n=10, columns=["embedding"])["source_id"]
    listings = listings_df[listings_df["source_id"].isin(source_id)]



# implement map
df = pd.DataFrame({
    "lon": listings["longitude"],
    "lat": listings["latitude"]
})

st.write(f"Nr of listings : {len(listings)}")

#st.map(df)


# Create the map with hover data
fig = px.scatter_mapbox(
    listings,
    lat="latitude",
    lon="longitude",
    hover_name="address",       # Column to display on hover
    hover_data=["price_man_yen", "area_sqm"], # Additional data to display on hover
    zoom=10,
    height=400,
    mapbox_style="carto-positron",
    size = "price_man_yen",
    size_max=10,
)

# Customize map layout
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# Display the map in Streamlit
st.plotly_chart(fig, use_container_width=True)


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
.image-container {
    width: 100%;
    height: 220px;
    overflow: hidden;
    border-radius: 10px;
}

.image-container img {
    width: 100%;
    height: 100%;
    object-fit: cover;
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
            st.markdown(
                f"""
                <div class="image-container">
                    <img src="{row['image_url']}">
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown(
            f"""
            <div class="price">{row.get('price_man_yen','')} 万円</div>
            <div class="meta">{row.get('area_sqm','')} m²</div>
            <div>{row.get('address','')}</div>
            """,
            unsafe_allow_html=True
        )

        st.markdown('</div>', unsafe_allow_html=True)
