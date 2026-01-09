import xml.etree.ElementTree as ET
import csv
import sys
import os

# --- CONFIGURATION ---
START_TAG = '<us-patent-grant'
END_TAG = '</us-patent-grant>'

def get_text_safe(element, path):
    """Helper to get text from a specific path safely."""
    if element is None:
        return ""
    found = element.find(path)
    return found.text.strip() if found is not None and found.text else ""

def extract_citations(root):
    """
    Extracts all citations into a single list, regardless of XML nesting.
    """
    data = {}
    
    # 1. Patent ID
    # We look recursively (.//) because structure varies between Utility vs Design patents
    doc_node = root.find(".//publication-reference//doc-number")
    data['patent_id'] = doc_node.text.strip() if doc_node is not None else "UNKNOWN"

    # 2. Extract Citations
    # We use .//us-references-cited to find the block ANYWHERE in the file
    # (It handles both Design patents where it's nested, and Utility where it's not)
    refs_cited = root.find('.//us-references-cited')
    
    cited_patents = []
    cited_npl = [] 
    
    if refs_cited is not None:
        # Loop through every <us-citation> tag inside the block
        for citation in refs_cited.findall('us-citation'):
            
            # A. Check for Patent Citation (<patcit>)
            patcit = citation.find('patcit')
            if patcit is not None:
                doc_num = get_text_safe(patcit, ".//document-id/doc-number")
                country = get_text_safe(patcit, ".//document-id/country")
                kind = get_text_safe(patcit, ".//document-id/kind")
                name = get_text_safe(patcit, ".//document-id/name")
                
                if doc_num:
                    # Create a clean string: US-7017193-B2 (Auger)
                    full_cite = f"{country}-{doc_num}-{kind}"
                    if name:
                        full_cite += f" ({name})"
                    cited_patents.append(full_cite)

            # B. Check for Non-Patent Literature (<nplcit>)
            nplcit = citation.find('nplcit')
            if nplcit is not None:
                othercit = get_text_safe(nplcit, ".//othercit")
                if othercit:
                    # Remove newlines to keep CSV clean
                    clean_npl = " ".join(othercit.split())
                    cited_npl.append(clean_npl)

    # 3. Join lists into single strings
    data['cited_patent_ids'] = " || ".join(cited_patents)
    data['cited_npl_text'] = " || ".join(cited_npl)
    data['total_citations'] = len(cited_patents) + len(cited_npl)
    
    # Debug print to verify it's working
    # print(f"Found {data['total_citations']} citations for {data['patent_id']}")

    return data

def parse_xml_to_csv(input_file, output_file):
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found.")
        return

    headers = ['patent_id', 'total_citations', 'cited_patent_ids', 'cited_npl_text']

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
                        row_data = extract_citations(root)
                        writer.writerow(row_data)
                        count += 1
                        
                        if count % 500 == 0:
                            print(f"Parsed {count} patents...", end='\r')
                            
                    except ET.ParseError:
                        pass 

    print(f"\nSuccess! Extracted citations for {count} patents to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        parse_xml_to_csv(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python3 extract_citations.py <input_file.xml> <output_file.csv>")