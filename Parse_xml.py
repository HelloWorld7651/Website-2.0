import xml.etree.ElementTree as ET
import csv
import sys
import os

# --- CONFIGURATION ---
# The tag that starts a new record
START_TAG = '<us-patent-grant'
# The tag that ends a record
END_TAG = '</us-patent-grant>'

def flatten_element(element, parent_key='', sep='.'):
    """Flattens the nested XML structure."""
    items = {}
    for name, value in element.attrib.items():
        items[f"{parent_key}{sep}@{name}" if parent_key else f"@{name}"] = value

    if element.text and element.text.strip():
        items[parent_key] = element.text.strip()

    for child in element:
        new_key = f"{parent_key}{sep}{child.tag}" if parent_key else child.tag
        # Handle list items by checking if key exists? 
        # For simplicity in this flattening logic, we allow overwrites or simple concatenation.
        # If highly complex lists exist, we might only capture the last one.
        items.update(flatten_element(child, new_key, sep=sep))
    return items

def parse_concatenated_xml(input_file, output_file):
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found.")
        return

    print(f"Processing: {input_file}")
    
    # --- PHASE 1: SCAN FOR HEADERS ---
    print("Phase 1: Scanning file to detect all columns (this ensures no data is lost)...")
    all_headers = set()
    
    # We read the file line by line as text
    with open(input_file, 'r', encoding='utf-8') as f:
        buffer = []
        inside_record = False
        count = 0
        
        for line in f:
            if START_TAG in line:
                inside_record = True
                buffer = [line] # Start fresh buffer
            elif inside_record:
                buffer.append(line)
                if END_TAG in line:
                    inside_record = False
                    # We have a full XML block in 'buffer'. Parse it.
                    xml_string = "".join(buffer)
                    try:
                        root = ET.fromstring(xml_string)
                        flat = flatten_element(root)
                        all_headers.update(flat.keys())
                        count += 1
                        if count % 500 == 0:
                            print(f"Scanned {count} patents...", end='\r')
                    except ET.ParseError:
                        pass # Skip broken blocks

    print(f"\nPhase 1 Complete. Detected {len(all_headers)} unique columns.")
    
    # Sort headers
    csv_headers = sorted(list(all_headers))

    # --- PHASE 2: WRITE DATA ---
    print("Phase 2: Writing data to CSV...")
    
    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        
        writer = csv.DictWriter(f_out, fieldnames=csv_headers)
        writer.writeheader()
        
        buffer = []
        inside_record = False
        count = 0
        
        for line in f_in:
            if START_TAG in line:
                inside_record = True
                buffer = [line]
            elif inside_record:
                buffer.append(line)
                if END_TAG in line:
                    inside_record = False
                    xml_string = "".join(buffer)
                    try:
                        root = ET.fromstring(xml_string)
                        row_data = flatten_element(root)
                        writer.writerow(row_data)
                        count += 1
                        if count % 100 == 0:
                            print(f"Written {count} rows...", end='\r')
                    except ET.ParseError:
                        print(f"Skipping a malformed record at index {count}")

    print(f"\nSuccess! Converted {count} patents to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 Parse_xml.py <input_file> <output_file>")
    else:
        parse_concatenated_xml(sys.argv[1], sys.argv[2])