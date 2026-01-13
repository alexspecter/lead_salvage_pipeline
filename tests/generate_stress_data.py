from lead_cleaner.phase0_setup.generator import GarbageGenerator
import os

def generate_stress_data():
    output_file = "input/stress_test.csv"
    print(f"Generating 500 rows to {output_file}...")
    
    # Using 500 to keep runtime reasonable for verification while still vetting logic
    # The generator creates ~10% duplicates/messy data usually
    gen = GarbageGenerator(seed=42)
    gen.generate_csv(output_file, count=500)
    print("Done.")

if __name__ == "__main__":
    generate_stress_data()
