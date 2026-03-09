# extract_pdf.py
import re

def extract_pdf(pdf_path):
    """
    Custom PDF extractor for poultry reports.
    Returns (metadata_dict, list_of_disease_dicts)
    """

    metadata = {}
    disease_rows = []

    # For your PDFs, we used PyPDF2 earlier, but you can also switch to pdfplumber if needed
    import PyPDF2
    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"

    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # ------------------------
    # Extract metadata
    # ------------------------
    for line in lines:
        if line.startswith("Lab Number:"):
            metadata["lab_number"] = line.split(":",1)[1].strip()
        elif line.startswith("Sample Date:"):
            metadata["sample_date"] = line.split(":",1)[1].strip()
        elif line.startswith("Client:"):
            metadata["client"] = line.split(":",1)[1].strip()
        elif line.startswith("Farm Name:"):
            metadata["farm"] = line.split(":",1)[1].strip()
        elif line.startswith("Address:"):
            metadata["address"] = line.split(":",1)[1].strip()
        elif line.startswith("Purpose of Sampling:"):
            metadata["purpose"] = line.split(":",1)[1].strip()
        elif line.startswith("Species:"):
            metadata["species"] = line.split(":",1)[1].strip()
        elif line.startswith("State Veterinarian:"):
            metadata["state_vet"] = line.split(":",1)[1].strip()

    # ------------------------
    # Extract disease blocks
    # ------------------------
    disease_pattern = re.compile(
        r"(?P<disease>[A-Za-z\s]+)\s+(?P<titre>\d+)\s+(?P<cv>\d+\.?\d*)\s+(?P<interpretation>\w+)"
    )

    for line in lines:
        m = disease_pattern.match(line)
        if m:
            disease_rows.append({
                "disease": m.group("disease").strip(),
                "titre": float(m.group("titre")),
                "cv": float(m.group("cv")),
                "interpretation": m.group("interpretation")
            })

    return metadata, disease_rows

# ------------------------
# Example usage
# ------------------------
if __name__ == "__main__":
    pdf_file = "953073_03031540090192.pdf"
    metadata, diseases = extract_pdf(pdf_file)
    print("Metadata:", metadata)
    print("Diseases:")
    for d in diseases:
        print(d)
