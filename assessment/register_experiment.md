gi```markdown
## Test one
1) Luxury = expensive vs cheap
2) Brightness = bright vs dark
3) Condition = new vs used

## Test two
2)In the build_labels(room_type: str, attribute: str) -> list[str]:
    templates = {
        "brightness": [
            f"a bright, well-lit {room_type} with natural light",
            f"a dark, poorly lit {room_type}",
        ],
        "luxury": [
            f"a photo of an expensive, luxurious {room_type} with premium finishes",
            f"a photo of a cheap, basic {room_type} with low-quality materials",
        ],
        "condition": [
            f"a photo of a brand new, renovated {room_type}",
            f"a photo of an old, worn-out {room_type} in poor condition",
        ],
    }
    ── LUXURY ──
  log_loss    : 0.6752
  accuracy    : 0.5932
  precision   : 0.6000
  recall      : 0.5172

── BRIGHTNESS ──
  log_loss    : 1.0939
  accuracy    : 0.5085
  precision   : 0.5000
  recall      : 0.9655

── CONDITION ──
  log_loss    : 1.9972
  accuracy    : 0.5424
  precision   : 0.5179
  recall      : 1.0000

## Test three
All pictures in a listing are bright so it could not detect

"brightness": [
            f"a bright, well-lit {room_type} with natural light",
            f"a dark, poorly lit {room_type}",
        ],
── BRIGHTNESS ──
  log_loss    : 0.7683
  accuracy    : 0.5593
  precision   : 0.7143
  recall      : 0.1724

## Test three
  "brightness":[
    f"a well-lit {room_type} with bright walls, large windows and daylight",
    f"a dark {room_type} with poor lighting, small windows and no light"
  ],
── BRIGHTNESS ──
  log_loss    : 0.7723416338429825
  accuracy    : 0.6779661016949152
  precision   : 0.65625
  recall      : 0.7241379310344828
  confusion_matrix: [19, 11, 8, 21]

## Test four Claude
Running with Claude using the following labels:
- luxury: 0.0 = cheap/basic (vinyl floors, stock cabinets, no detail),
1.0 = high-end (marble, bespoke millwork, designer fixtures, herringbone parquet, statement chandelier)
- condition:  0.0 = severe damage (peeling paint, water stains, cracked tiles, mold),
1.0 = pristine/immaculate (spotless, freshly renovated, zero blemishes)
- brightness: 0.0 = dark/dim (no windows, heavy curtains, dim bulb),
1.0 = sun-drenched (floor-to-ceiling glass, skylights, flooded with natural light, luminous)
- spaciousness: 0.0 = cramped/cluttered (low ceiling, blocked pathways),
1.0 = voluminous/grand (double-height, sweeping open-plan, expansive sightlines)

## Test four Claude
We work with rankings
Guidelines:
- luxury:
0.0–0.2 = builder-grade laminate, hollow-core doors, vinyl flooring, basic white fixtures, stock cabinets, no architectural detail,
0.3–0.5 = mid-range finishes, ceramic tile, standard granite counters, basic stainless appliances, simple crown molding,
0.6–0.8 = premium hardwood floors, quartz countertops, custom cabinetry, designer light fixtures, high-end appliances (Sub-Zero, Wolf),
0.9–1.0 = marble/travertine surfaces, bespoke millwork, coffered ceilings, statement chandelier, integrated smart home, herringbone parquet, designer fixtures (Waterworks, Restoration Hardware)

- condition:
0.0–0.2 = peeling paint, water stains on ceiling, cracked tiles, broken fixtures, warped floors, visible mold, dirty grout,
0.3–0.5 = minor scuffs on walls, slightly worn flooring, dated but functional fixtures, some discoloration,
0.6–0.8 = well-maintained, clean surfaces, no visible damage, fresh paint, intact fixtures, tidy and orderly,
0.9–1.0 = pristine, immaculate, show-home condition, spotless grout, zero blemishes, freshly renovated, like new

- brightness:
0.0–0.2 = no windows visible, artificial light only, dim overhead bulb, dark corners, heavy blackout curtains,
0.3–0.5 = some natural light, partially shaded, small windows, moderate artificial lighting,
0.6–0.8 = good natural light, large windows, bright and airy feel, south-facing exposure,
0.9–1.0 = floor-to-ceiling windows, sun-drenched, flooded with natural light, skylights, panoramic glass, glowing interior

- spaciousness:
0.0–0.2 = cramped, cluttered, low ceiling, furniture blocking pathways, no circulation space, tight corridors,
0.3–0.5 = average room size, standard ceiling height, functional but not generous, modest proportions,
0.6–0.8 = open floor plan, generous proportions, good circulation, 10ft+ ceilings, minimal visual clutter,
0.9–1.0 = grand, voluminous, double-height ceilings, sweeping xopen plan, loft-like, unobstructed sightlines, expansive
"""

```
```
