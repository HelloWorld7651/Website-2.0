import xml.etree.ElementTree as ET
import csv
import sys
import os

# --- CONFIGURATION ---
# Updated tags for Patent Applications
START_TAG = '<us-patent-application'
END_TAG = '</us-patent-application>'

def get_text_safe(element, path):
    """Helper to get text from a specific path safely."""
    found = element.find(path)
    return found.text.strip() if found is not None and found.text else ""

def extract_application_data(root):
    """
    Extracts specific columns from the us-patent-application root.
    """
    data = {}

    # 1. Patent ID & Date
    # Path: us-bibliographic-data-application -> publication-reference
    biblio = root.find('us-bibliographic-data-application')
    if biblio is not None:
        data['patent_id'] = get_text_safe(biblio, ".//publication-reference//doc-number")
        data['date'] = get_text_safe(biblio, ".//publication-reference//date")
        data['title'] = get_text_safe(biblio, ".//invention-title")
        
        # 2. Classifications (CPC first, fallback to IPCR)
        cpc_sec = get_text_safe(biblio, ".//classifications-cpc/main-cpc//section")
        if not cpc_sec:
            cpc_sec = get_text_safe(biblio, ".//classifications-ipcr/classification-ipcr/section")
        
        data['section'] = cpc_sec

    # 3. Abstract
    abstract = root.find('abstract')
    if abstract is not None:
        paras = [p.text.strip() for p in abstract.findall('p') if p.text]
        data['abstract'] = " ".join(paras)
    else:
        data['abstract'] = ""

    # 4. Claims
    claims_node = root.find('claims')
    if claims_node is not None:
        claim_texts = []
        for claim in claims_node.findall('claim'):
            # Join all text recursively inside the claim
            text = "".join(claim.itertext())
            claim_texts.append(text.strip())
        
        data['claims_text'] = " || ".join(claim_texts)
        data['num_claims'] = len(claim_texts)
    else:
        data['claims_text'] = ""
        data['num_claims'] = 0

    return data

def parse_xml_to_csv(input_file, output_file):
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found.")
        return

    # Define the columns we want in our CSV
    headers = ['patent_id', 'date', 'section', 'num_claims', 'title', 'abstract', 'claims_text']

    print(f"Processing: {input_file}")
    print("Writing to CSV...")

    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        
        writer = csv.DictWriter(f_out, fieldnames=headers)
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
                        row_data = extract_application_data(root)
                        writer.writerow(row_data)
                        count += 1
                        
                        if count % 500 == 0:
                            print(f"Parsed {count} applications...", end='\r')
                            
                    except ET.ParseError:
                        pass 

    print(f"\nSuccess! Extracted {count} applications to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        parse_xml_to_csv(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python3 parse_applications.py <input.xml> <output.csv>")