import json
import os
import random
import string

def generate_dependents():
    dependents = []
    for _ in range(random.randint(0, 2)):
        dependent = {
            "name": ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=8)),
            "relation": random.choice(["Spouse", "Child", "Parent", "Sibling", "Grandparent"]),
            "dob": f"{random.randint(1950, 2010)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        }
        dependents.append(dependent)
    return dependents

def generate_copay():
    copay_types = [
        {"type": "Primary Care", "amount": random.randint(10, 30)},
        {"type": "Specialist", "amount": random.randint(40, 60)},
        {"type": "Emergency Room", "amount": random.randint(100, 200)},
        {"type": "Urgent Care", "amount": random.randint(50, 80)},
        {"type": "Physical Therapy", "amount": random.randint(30, 70)},
        {"type": "Mental Health", "amount": random.randint(50, 100)},
        {"type": "Diagnostic Test", "amount": random.randint(20, 50)},
        {"type": "Prescription Drugs", "amount": random.randint(5, 30)},
        {"type": "Home Health Care", "amount": random.randint(20, 60)},
        {"type": "Hospice Care", "amount": random.randint(0, 50)},
        {"type": "Skilled Nursing Facility", "amount": random.randint(50, 100)},
        {"type": "Durable Medical Equipment", "amount": random.randint(20, 80)},
        {"type": "Preventive Services", "amount": random.randint(0, 20)},
        {"type": "Rehabilitation Services", "amount": random.randint(30, 70)},
        {"type": "Ambulance Services", "amount": random.randint(100, 200)},
        {"type": "Vision Care", "amount": random.randint(10, 40)},
        {"type": "Dental Services", "amount": random.randint(20, 60)}
    ]
    return random.sample(copay_types, random.randint(1, len(copay_types)))

def generate_claim():
    return {
        "claim_number": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
        "date_of_service": f"{random.randint(2020, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        "provider": ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=10)),
        "diagnosis": random.sample(["COVID-19", "Fracture", "Heart Attack", "Appendicitis", "Pneumonia", "Diabetes", "Arthritis", "Stroke", "Cancer", "Hypertension", "Allergy", "Migraine", "Obesity", "Depression"], random.randint(1, 3)),
        "procedure": random.sample(["MRI", "X-Ray", "Surgery", "Physical Therapy", "Blood Test", "Colonoscopy", "Dental Cleaning", "Eye Exam", "Vaccination", "Counseling", "Ultrasound", "Endoscopy", "Chiropractic Adjustment", "Prenatal Care"], random.randint(1, 2)),
        "billed_amount": random.randint(100, 1000),
        "covered_amount": random.randint(50, 500),
        "patient_responsibility": random.randint(0, 100)
    }

def generate_sample_file(index):
    data = {
        "policy_number": f"POL{index}",
        "policy_holder": {
            "name": ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=8)),
            "dob": f"{random.randint(1950, 2000)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "gender": random.choice(["Male", "Female"]),
            "address": {
                "street": ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=10)),
                "city": ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5)),
                "state": random.choice(["NY", "CA", "TX", "FL", "IL", "PA", "OH", "MI", "GA", "WA", "NC", "VA", "NJ", "MA", "AZ"]),
                "zipcode": ''.join(random.choices(string.digits, k=5))
            }
        },
        "dependents": generate_dependents(),
        "coverage": {
            "plan": random.choice(["Gold Plan", "Silver Plan", "Bronze Plan"]),
            "effective_date": f"{random.randint(2020, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "expiry_date": f"{random.randint(2024, 2026)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "deductible": random.randint(500, 2000),
            "copay": generate_copay(),
            "max_coverage_amount": random.randint(5000, 20000)
        },
        "claims": [generate_claim() for _ in range(random.randint(1, 5))]
    }

    filename = f"sample_{index}.json"
    filepath = os.path.join("sample_files", filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Generated sample file: {filename}")

# Create directory to store sample files
os.makedirs("sample_files", exist_ok=True)

# Generate 100 sample files
for i in range(1, 101):
    generate_sample_file(i)
