# Realtime Baidu Services

Use `simple_baidu_services.py` for ordinary realtime road-traffic, route, weather, and geocoding requests.

## Actions

| Action | Use For | Required | Optional / Defaults |
| --- | --- | --- | --- |
| `route` | Route planning | `origin`, `destination` | `mode=driving`, `city`; modes: `driving`, `walking`, `transit`, `riding` |
| `weather` | Weather query | `location` | `city`; script resolves Baidu `district_id` internally |
| `geocode` | Address, POI, or road name to coordinate | `address` | `city`, `ret_coordtype=bd09ll` |
| `road-traffic` | Realtime traffic for one named road | `road_name`, `city` | `horizon_minutes=30` |
| `around-traffic` | Realtime traffic around a coordinate | `center` | `radius=500`, `road_grade=0`, `coord_type_input=bd09ll`, `coord_type_output=bd09ll` |

## Workflow

1. Call `--action capabilities` first and treat it as the source of truth.
2. Choose one direct action when required fields are present.
3. Use a chain only when nearby traffic needs coordinates:
   `geocode(address, city)` -> `around-traffic(center=<geocode.location.center>)`.
4. Ask only for missing required fields or necessary disambiguation.
5. Fill optional parameters with defaults unless the user specifies them.

## Intent Distinctions

- Use `road-traffic` when the user asks about one named road, such as `朱雀大街这条路的拥堵情况`.
- Use `around-traffic` when the user asks about nearby/surrounding traffic, such as `太乙路周边路况` or `大雁塔附近1公里路况`.
- If a road/place name is ambiguous and no city can be inferred, ask for `city`.
- Do not ask for `radius`, `road_grade`, or coordinate type by default.

## Commands

```bash
# weather
python /mnt/skills/custom/road-traffic-analysis/scripts/simple_baidu_services.py --action weather --location "西安市雁塔区"

# route
python /mnt/skills/custom/road-traffic-analysis/scripts/simple_baidu_services.py --action route --origin "西安钟楼" --destination "西安北站" --mode driving --city "西安市"

# geocode
python /mnt/skills/custom/road-traffic-analysis/scripts/simple_baidu_services.py --action geocode --address "西安钟楼" --city "西安市"

# single-road realtime traffic
python /mnt/skills/custom/road-traffic-analysis/scripts/simple_baidu_services.py --action road-traffic --road-name "太乙路" --city "西安市"

# around traffic from coordinate
python /mnt/skills/custom/road-traffic-analysis/scripts/simple_baidu_services.py --action around-traffic --center "34.245,108.945" --radius 500
```

## Output

Return a concise answer with action(s), input parameters, and key result summary. On failure, include exact `missing_fields` or `error`.
