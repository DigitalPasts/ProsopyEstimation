"""
estimation.py

This module contains functions for estimating dates of historical documents based on known data
from the 'yellow pages' dataset. The estimation is performed using a weighted average approach
that incorporates known years of activity for individuals mentioned in dated documents.

Main functionalities:
- estimate_date: Estimates the date of one document based on co-occurrences with dated individuals
- iterative_date_estimation: 

"""
import numpy as np
import pandas as pd
from yellow_pages import YellowPages

from helpers import get_dateless_rows

preprocessed_prosobab_data = "../data/input/preprocessed_whole_data.csv"

max_active_years = 17

def estimate_date(group, yellow_pages, max_active_years=30):
    def get_valid_people(group, yp, max_years):
        valid = []
        for pid in group['PID']:
            data = yp.get_person_data(pid)
            if not data:
                continue
            from_year = data.get('attested_from_year') or data.get('estimated_from_year')
            to_year = data.get('attested_to_year') or data.get('estimated_to_year')
            if from_year is not None and to_year is not None:
                if abs(to_year - from_year) < max_years:
                    valid.append(pid)
        return valid

    known_people = []
    # Tiered fallback thresholds (e.g., 30, 60, 90)
    thresholds = [max_active_years, max_active_years * 2, max_active_years * 3]
    for threshold in thresholds:
        known_people = get_valid_people(group, yellow_pages, threshold)
        if known_people:
            break

    if not known_people:
        return None, None

    attested_from_years = []
    attested_to_years = []
    estimated_from_years = []
    estimated_to_years = []
    attested_weights = []
    estimated_weights = []

    for pid in known_people:
        data = yellow_pages.get_person_data(pid)
        if data['attested_from_year'] is not None and data['attested_to_year'] is not None:
            attested_from_years.append(data['attested_from_year'])
            attested_to_years.append(data['attested_to_year'])
            attested_weights.append(data['doc_count'])
        elif data['estimated_from_year'] is not None and data['estimated_to_year'] is not None:
            estimated_from_years.append(data['estimated_from_year'])
            estimated_to_years.append(data['estimated_to_year'])
            estimated_weights.append(data['doc_count'])

    if attested_from_years and attested_to_years:
        estimated_from = np.average(attested_from_years, weights=attested_weights)
        estimated_to = np.average(attested_to_years, weights=attested_weights)
    elif estimated_from_years and estimated_to_years:
        estimated_from = np.average(estimated_from_years, weights=estimated_weights)
        estimated_to = np.average(estimated_to_years, weights=estimated_weights)
    else:
        return None, None

    return estimated_from, estimated_to


def iterative_date_estimation(dateless_rows, yellow_pages):
    estimated = set()
    total_estimates = 0
    dateless_rows['estimated_from_year'] = None
    dateless_rows['estimated_to_year'] = None
    est_ranges = []

    while True:
        documents = dateless_rows.copy().groupby('Tablet ID')
        new_estimates = 0
        for tab_id, tablet in documents:
            if tab_id in estimated:
                continue  # Skip already estimated tablets

            estimated_from, estimated_to = estimate_date(tablet, yellow_pages, max_active_years=max_active_years)
            if estimated_from is not None and estimated_to is not None:
                # Update the group's estimation
                dateless_rows.loc[tablet.index, 'estimated_from_year'] = estimated_from
                dateless_rows.loc[tablet.index, 'estimated_to_year'] = estimated_to
                est_ranges.append(estimated_to-estimated_from)

                # Add or update people in the yellow pages based on estimation
                for index in tablet.index:
                    # print(group.at[index, 'PID'], group.at[index, 'Name'])
                    pid = tablet.at[index, 'PID']
                    person_data = yellow_pages.get_person_data(pid)
                    if not person_data:
                        # Add new person with estimated years
                        yellow_pages.add_person(
                            name=pid,  # Assuming 'person_id' serves as a unique identifier
                            ind_name=tablet.at[index,'ind.Name'],
                            roles=[tablet.at[index,'Role']],  # Gather roles from the group
                            from_year_est=estimated_from,
                            to_year_est=estimated_to,
                        )
                    else:
                        # Update existing person's estimated years
                        yellow_pages.update_person(pid, tablet.at[index,'Role'], estimated_from, estimated_to)
                #
                estimated.add(tab_id)
                new_estimates += 1
                total_estimates += 1

        print(f"New estimates in this round: {new_estimates}")
        if new_estimates == 0:
            break  # Exit loop when no new estimates are made

    unestimated_docs = dateless_rows[dateless_rows['estimated_from_year'].isna()]
    remaining_tablets = unestimated_docs['Tablet ID'].nunique()

    print(f"📄 Remaining unestimated documents: {remaining_tablets}")
    print(f"📊 Average range of estimation: {np.average(est_ranges):.2f}")
    print(f"✅ Total documents estimated: {total_estimates}")

    return dateless_rows


def estimate(output_path="../data/output/estimation_results.csv"):
    """
    Run the estimation process and save results to CSV.

    The output CSV contains all tablets with their status:
    - 'pre_dated': tablets that had a date from the beginning
    - 'newly_estimated': tablets that were dated during estimation
    - 'unestimatable': tablets that could not be dated
    """
    df = pd.read_csv(preprocessed_prosobab_data).copy()
    df['Julian date'] = df['Julian date'].fillna('-')
    df['Split_Julian_dates'] = df['Julian date'].str.split(pat="/").str[0]

    # Identify pre-dated tablets (those with valid Julian dates)
    pre_dated_mask = df['Split_Julian_dates'].str.match(r'^\d{3,4}$', na=False)
    pre_dated_tablets = set(df.loc[pre_dated_mask, 'Tablet ID'].unique())
    all_tablets = set(df['Tablet ID'].unique())

    yp = YellowPages(df)
    dateless_rows = get_dateless_rows(df)
    final_dateless_rows = iterative_date_estimation(dateless_rows, yp)

    # Identify newly estimated tablets
    estimated_mask = final_dateless_rows['estimated_from_year'].notna()
    newly_estimated_tablets = set(final_dateless_rows.loc[estimated_mask, 'Tablet ID'].unique())

    # Identify unestimatable tablets
    unestimatable_tablets = all_tablets - pre_dated_tablets - newly_estimated_tablets

    # Build results dataframe
    results = []

    # Pre-dated tablets
    for tab_id in pre_dated_tablets:
        tablet_rows = df[df['Tablet ID'] == tab_id]
        julian_date = tablet_rows['Split_Julian_dates'].iloc[0]
        results.append({
            'Tablet ID': tab_id,
            'status': 'pre_dated',
            'julian_date': julian_date,
            'estimated_from': None,
            'estimated_to': None
        })

    # Newly estimated tablets
    for tab_id in newly_estimated_tablets:
        tablet_rows = final_dateless_rows[final_dateless_rows['Tablet ID'] == tab_id]
        est_from = tablet_rows['estimated_from_year'].iloc[0]
        est_to = tablet_rows['estimated_to_year'].iloc[0]
        results.append({
            'Tablet ID': tab_id,
            'status': 'newly_estimated',
            'julian_date': None,
            'estimated_from': est_from,
            'estimated_to': est_to
        })

    # Unestimatable tablets
    for tab_id in unestimatable_tablets:
        results.append({
            'Tablet ID': tab_id,
            'status': 'unestimatable',
            'julian_date': None,
            'estimated_from': None,
            'estimated_to': None
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)

    print(f"\n📁 Results saved to {output_path}")
    print(f"   Pre-dated tablets: {len(pre_dated_tablets)}")
    print(f"   Newly estimated tablets: {len(newly_estimated_tablets)}")
    print(f"   Unestimatable tablets: {len(unestimatable_tablets)}")

    return results_df
