import json
import os
import re
from src.gen.real_estate.listing_events_pb import ListingIngestedEvent
from src.gen.real_estate.real_estate_listing_pb import RealEstateListing
from src.gen.real_estate.metadata_pb import ext_info, FieldSection

json_path = os.path.join(
    os.path.dirname(__file__), 
    "..", "gen", "real_estate", "real_estate_listing_section_startings.json"
)

with open(json_path, "r") as f:
    SECTION_STARTINGS = json.load(f)

def build_intelligence_context(payload: ListingIngestedEvent) -> str:
    categorized = {section: [] for section in FieldSection}
    
    for field in RealEstateListing.desc().fields:
        value = getattr(payload.listing, field.name, None)
        
        if value is None or value == "" or value == 0 or value is False:
            continue
        
        if field.proto.options is None:
            continue
        
        metadata = field.proto.options[ext_info]
        if not metadata or not metadata.template:
            continue
        
        section = metadata.section
        template = metadata.template
        
        if section == FieldSection.SECTION_SKIP:
            continue
            
            
        categorized[section].append(template.replace("##", str(value).title()))
    
    parts = []
    
    for section in sorted(FieldSection, key=lambda s: s.value):
        if section == FieldSection.SECTION_SKIP:
            continue
            
        items = categorized[section]
        if not items:
            continue
            
        start_str = SECTION_STARTINGS.get(section.name, "")
        
        if section.name == "SECTION_INDIVIDUAL_CONFIG":
            section_text = " ".join(items)
        else:
            section_text = ", ".join(items)
            
        if start_str:
            part = f"{start_str} {section_text}"
        else:
            part = section_text
            
        # Capitalize only the first letter of the whole part
        if part:
            parts.append(part[0].upper() + part[1:])
            
    paragraph = ". ".join(p.rstrip('.') for p in parts)
    
    paragraph = re.sub(r'\s+', ' ', paragraph).strip()
    
    return paragraph[0].upper() + paragraph[1:] + "." if paragraph else "."
