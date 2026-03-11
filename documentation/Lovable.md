```markdown
# Lovable prompt

I want to create an interface for a website for real state agents. This website will display images of the apartments, its locations, Score Price, Market Price derived from the listings-CSV mentioned below.

I want to be able to click on the apartment and get a general apartment brightness score, a luxury score, a spaciousness score, a condition score. When clicking on the address, it should be linked to a google maps location of this apartment using its longitute and latitude.

I also want to include the possibilitiy to search and filter the apartments for all variables including these scores at the very top. It should accept searches and be able to filter for combinations such as "Expensive kitchen" (i.e. >50% luxury score for kitchen), "Bright bedroom" (i.e. >50% brightness score for bedroom), "New toilet" (i.e. >50% condition score for toilet), “Spacious living room” for each room type (kitchen, bedroom, living room, bathroom and kitchen) as well as the opposite adjective (i.e. cheap, dark, used, small) corresponding to ≤50% score.

Additionally, just below the search bar and above the display of images of the apartments, I want an interactive map of Tokyo which indicates the locations of all apartments fitting search results.

The app should receive a listings-CSV where one row corresponds to one listing and with the following column names and they will additionally have a column with the predicted price:

level_0, source_id, url, price_man_yen, area_sqm, year_built, floor_number, floors_total, address, nearest_station, walk_minutes, image_count, rooms_num, base_layout, luxury_bathroom, luxury_bedroom, luxury_kitchen, luxury_living_room, luxury_toilet, brightness_bathroom, brightness_bedroom, brightness_kitchen, brightness_living_room, brightness_toilet condition_bathroom, condition_bedroom, condition_kitchen, condition_living_room, condition_toilet, longitude, latitude

The app should receive an additional image-CSV where one row corresponds to one image and with the following source_id, listing_url, image_url, image_name, image_path, default_image, room_type, scoring_dict. The column “source_id” links each image from the second CSV to a listing from the first CSV.

# Change prompts
Please adjust the depiction of the scores as float with 2 decimals. Please adjust the Search to correspond to the scores between 0 and 1 instead of 0 and 100, e.g. Bright bedroom should result in all listings with a bedroom brightness score of >0.50. Please make an option to hide the map by scrolling past the map. Please change the format of the listings into a grid with cards in three columns (1 card = 1 listing).

```
