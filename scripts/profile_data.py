import pandas as pd
import argparse
from ydata_profiling import ProfileReport
import os


def main(input_file):
    if not os.path.exists(input_file):
        raise FileExistsError(f"File not found: {input_file}")

    # Load dataset
    df = pd.read_csv(input_file)

    # Create profile report
    profile = ProfileReport(
        df, title='Movie Database Profile Report', explorative=True)

    # Save Report
    os.makedirs('reports', exist_ok=True)
    output_path = 'reports/movie_data_profile.html'
    profile.to_file(output_path)

    print(f"Data profiling report saved to {output_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generating a profiling report for a dataset.')
    parser.add_argument('--input_file', type=str,
                        default='data/processed/movie_metadata_cleaned.csv', help='Path to input CSV file')

    args = parser.parse_args()
    main(args.input_file)
