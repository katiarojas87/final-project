import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
from embeddings import load_clip_model, get_text_embeddings, get_similarity
import base64

@st.cache_resource
def get_base64_image(path):
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

logo_base64 = get_base64_image(Path.cwd()/"frontend/GoodDeal.png")

st.set_page_config(layout="wide")

@st.cache_resource
def get_clip():
    return load_clip_model()

@st.cache_resource
def get_images():
    df = pd.read_csv(Path.cwd() / "frontend/images_cleaned_embedding.csv", nrows=8000)
    return df[df["embedding"] != "[]"]

@st.cache_resource
def get_listings():
    df = pd.read_csv(Path.cwd() / "frontend/listings_with_deal.csv")
    images_df = get_images()
    source_id = images_df["source_id"]
    df = df[df["source_id"].isin(source_id)]
    df["difference"] = (df["predicted_price"] - df["price_man_yen"])/df["price_man_yen"]
    df = df[df["difference"] != 0.0]
    df.loc[df["difference"] < 0.05, "deal"] = "Regular Price"

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

'''
### Image search
'''
query = st.text_input("Write a prompt, e.g. kitchen with island", value="")

'''
### Set filters
'''
col1, col2, col3 = st.columns(3)

with col1:
    max_price = st.number_input("Maximum price 万円", min_value=1000, max_value=50000, value=10000, step=1)

with col2:
    min_area = st.number_input("Minimum area m²", min_value=0, max_value=270, value = 15, step=0)

with col3:
    min_year = st.number_input("Minimum year built", min_value=1960, max_value=2024, step=1)

show_all = st.selectbox("Show Good Deals only?", ["Show all", "Good Deal only"])


st.markdown('''

''')

# Apply filter
listings_df = listings_df[listings_df["price_man_yen"] <= max_price]
listings_df = listings_df[listings_df["area_sqm"] >= min_area]
listings_df = listings_df[listings_df["year_built"] >= min_year]

if show_all == "Good Deal only":
    listings_df = listings_df[listings_df["deal"] == "Good Deal"]

# Text embedding
text_embedding = get_text_embeddings(model, processor, [query])

source_id = listings_df["source_id"]
images_df = images_df[images_df["source_id"].isin(source_id)]
similarity = images_df["embedding"].apply(lambda x: \
        get_similarity(x, text_embedding)).astype("float").to_frame()

if query == "":
    listings = listings_df
else:
    similarity["source_id"] = images_df["source_id"]
    similarity["image_url"] = images_df["image_url"]
    source_id = similarity.nlargest(n=10, columns=["embedding"])["source_id"]
    listings = listings_df[listings_df["source_id"].isin(source_id)]

listings = listings.reset_index().drop(columns="index")

# Nr of listings shown
st.write(f"Nr of listings : {len(listings)}")


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

}
.image-container {
    width: 100%;
    height: 220px;
    overflow: hidden;

}
.image-container img {
    width: 100%;
    height: 100%;
    object-fit: cover;
}
.address-overlay {
    position: absolute;
    top: 8px;
    left: 8px;
    background: rgba(0,0,0,0.6);
    color: white;
    padding: 4px 8px;
    font-size: 13px;
    border-radius: 6px;
}
.container {
  display: flex; /* Makes the container a flex container */
  gap: 20px; /* Adds space between columns */
  border-width: 10px;
}
.good {
    background: #50C867;
    border-width: 10px;
    border-color: rgba(1, 153, 0, 0.5);
}
.column {
  padding: 15px;
}
.left {
    width: 60%; /* Sets the width of the first column */
}

.right {
    width: 40%; /* Sets the width of the second column */
}
.price {
    font-size: 22px;
    font-weight: bold;
}
.logo img {
    height: 120px;
}
.meta {
    color: #555;
}
</style>
""", unsafe_allow_html=True)


# --- Display Listings ---
cols = st.columns(3)

for i, row in listings.iterrows():

    logo_html = (
        f'<img src="data:image/png;base64,{logo_base64}">'
        if row.get("deal") == "Good Deal"
        else '<div class="meta"></div>'
    )
    container_def = (
        f'<div class="container good">'
        if row.get("deal") == "Good Deal"
        else f'<div class="container">'
    )

    col = cols[i % 3]

    with col:

        st.markdown('<div class="card">', unsafe_allow_html=True)

        if pd.notna(row.get("image_url")):
            st.markdown(
                f"""
                <div class="image-container">
                    <a href="{row.get('url','')}" target="_blank" style="text-decoration:none; color:inherit;">
                        <img src="{row['image_url']}">
                        <div class="address-overlay">{row.get('address','')}</div>
                    </a>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown(
            f"""
            {container_def}
                <div class="column left">
                    <div class="price">{row.get('price_man_yen','')} 万円</div>
                    <div class="meta">{row.get('predicted_price','')} 万円 (predicted)</div>
                    <div class="meta">built in {row.get('year_built','')}</div>
                    <div class="meta">{row.get('area_sqm','')} m²</div>
                    <div class="meta">{row.get('walk_minutes','')} min to {row.get('nearest_station','')}</div>
                </div>
                <div class="column right">
                    <div class="logo">
                        {logo_html}
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        st.markdown('</div>', unsafe_allow_html=True)



'''
## Location

'''
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
    color = "deal",
    size = "price_man_yen",
    size_max=10,
)

# Customize map layout
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# Display the map in Streamlit
st.plotly_chart(fig, use_container_width=True)
