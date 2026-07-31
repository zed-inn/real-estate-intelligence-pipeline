from gen.events_pb import PropertyIngestedEvent, ext_info, FieldCategory

CATEGORY_ORDER = [
    FieldCategory.CATEGORY_LOCATION,
    FieldCategory.CATEGORY_PRICING,
    FieldCategory.CATEGORY_SPEC,
    FieldCategory.CATEGORY_INFRASTRUCTURE,
    FieldCategory.CATEGORY_PROXIMITY,
    FieldCategory.CATEGORY_SCORE
]

def build_intelligence_context(payload: PropertyIngestedEvent) -> str:
    categorized: dict[FieldCategory, list[str]] = {cat: [] for cat in CATEGORY_ORDER}
    amenities: list[str] = []
    security: list[str] = []
    
    for field in PropertyIngestedEvent.desc().fields:
        value = getattr(payload, field.name, None)
        
        if value is None or value == "" or value == 0 or value is False:
            continue
        
        if field.proto.options is None:
            continue
        
        metadata = field.proto.options[ext_info]
        if not metadata or not metadata.natural_tone:
            continue
        
        category = metadata.category
        tone = metadata.natural_tone
        
        if category == FieldCategory.CATEGORY_SKIP:
            continue
        
        if category == FieldCategory.CATEGORY_AMENITY:
            if value is True:
                amenities.append(tone)
            continue
        
        if category == FieldCategory.CATEGORY_SECURITY:
            if value is True:
                security.append(tone)
            continue
        
        if category in categorized:
            categorized[category].append(tone.replace("##", str(value)))
    
    parts: list[str] = []
    for cat in CATEGORY_ORDER:
        parts.extend(categorized[cat])
    
    paragraph = ", ".join(parts)
    
    if amenities:
        paragraph += ". Amenities include " + ", ".join(amenities)
    
    if security:
        paragraph += ". Security features include " + ", ".join(security)
    
    return paragraph[0].upper() + paragraph[1:] + "." if paragraph else "."
