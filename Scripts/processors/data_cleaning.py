import re

def clean_text(text):
    if not text:
        return None
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    return text.title()

def clean_list_column(cell):
    if not cell:
        return []
    items = [x.strip().title() for x in cell.split(',') if x.strip()]
    return list(set(items)) 
