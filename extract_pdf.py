import pdfplumber

def extract_pdf(pdf_path):
    """
    Extracts metadata and disease rows from a PDF report.
    Returns a tuple: (metadata_dict, list_of_disease_dicts)
    """
    metadata = {}
    disease_rows = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = [line.strip() for line in text.split("\n") if line.strip()]

            # Example: Extract metadata (adjust keys to match your PDF)
            for line in lines:
                if line.startswith("Lab Number:"):
                    metadata["lab_number"] = line.split(":", 1)[1].strip()
                elif line.startswith("Sample Date:"):
                    metadata["sample_date"] = line.split(":", 1)[1].strip()
                elif line.startswith("Client:"):
                    metadata["client"] = line.split(":", 1)[1].strip()
                elif line.startswith("Farm Name:"):
                    metadata["farm"] = line.split(":", 1)[1].strip()
                elif line.startswith("Address:"):
                    metadata["address"] = line.split(":", 1)[1].strip()
                elif line.startswith("Purpose of Sampling:"):
                    metadata["purpose"] = line.split(":", 1)[1].strip()
                elif line.startswith("Species:"):
                    metadata["species"] = line.split(":", 1)[1].strip()
                elif line.startswith("State Veterinarian:"):
                    metadata["state_vet"] = line.split(":", 1)[1].strip()

            # Example: Extract disease table rows
            # Assuming each disease row has: Disease, Avg Titre, CV %, Interpretation
            for line in lines:
                parts = line.split()
                if len(parts) >= 4:
                    # Simple heuristic: last 3 parts are titre, cv, interpretation
                    try:
                        titre = float(parts[-3])
                        cv = float(parts[-2])
                        interpretation = parts[-1]
                        disease_name = " ".join(parts[:-3])

                        disease_rows.append({
                            "disease": disease_name,
                            "titre": titre,
                            "cv": cv,
                            "interpretation": interpretation
                        })
                    except ValueError:
                        continue  # not a disease row

    return metadata, disease_rows


# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    pdf_file = "953073_03031540090192.pdf"
    metadata, diseases = extract_pdf(pdf_file)

    print("Metadata:")
    print(metadata)
    print("\nDiseases:")
    for d in diseases:
        print(d)
