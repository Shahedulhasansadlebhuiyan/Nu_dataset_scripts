

import json
from datasets import load_dataset

OUTPUT_PATH = "fineweb_nuclear_data.jsonl"

NUCLEAR_KEYWORDS = {
    "fission", "fusion", "uranium-235", "radioisotope", "neutron flux",
    "reactor core", "tokamak", "half-life", "iaea", "dosimetry",
    "breeder reactor", "nuclear physics", "radioactive decay"
}

# Terms that often indicate non-technical or irrelevant content
EXCLUSION_TERMS = ["nuclear family", "nuclear option", "nuclear cell"]

def is_high_quality_nuclear(example):
    text = example['text'].lower()

    # Condition 1: High educational value (using FineWeb-Edu's internal score)
    if example['score'] < 3:
        return False

    # Condition 2: Check for negative keywords (metaphors/politics)
    if any(term in text for term in EXCLUSION_TERMS):
        return False

    # Condition 3: Check for technical keyword density
    matches = [word for word in NUCLEAR_KEYWORDS if word in text]
    return len(matches) >= 2

# 4. Stream and Filter
print("Initializing Stream from Hugging Face...")
# We use the 'sample' config to test quickly; change to 'default' for the full 1.3T dataset
ds = load_dataset("HuggingFaceFW/fineweb-edu", name="sample-350BT", split="train", streaming=True)

print(f"Filtering started. Results will be saved to: {OUTPUT_PATH}")

count = 0
limit = 5000 # Set a limit for your first run to test quality

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    for example in ds:
        if is_high_quality_nuclear(example):
            f.write(json.dumps(example) + "\n")
            count += 1

            if count % 50 == 0:
                print(f"Found {count} high-quality technical docs...")

        if count >= limit:
            break

print(f"Success! {count} nuclear science documents exported to your Google Drive.")