"""
validation process is as follows:
given only the dated tablets df, each dated tablet is excluded from the df and gets its date estimated using the
core/estimation/estimate_date function. The true date is compared with the estimation, for two measurements:
1. binary success/fail - if the true date falls within the range, it is recorded as success, otherwise failure.
2. the distance of the true date from the center of the estimated range (coe)

The function reports on:
1. the success rates
2. standard deviation from coe for all tablets
3. standard deviation from coe for failed tablets

"""
import pandas as pd
from tqdm import tqdm
from helpers import get_fully_dated_rows_by_julian
from yellow_pages import YellowPages

from estimation import estimate_date

preprocessed_prosobab_data = "../data/input/preprocessed_whole_data.csv"

def summarize_validation_results(results_df):
    # Rows where an estimate exists
    estimated_df = results_df.dropna(subset=['estimated_from', 'estimated_to'])

    total = len(results_df)
    total_estimated = len(estimated_df)
    not_estimated = total - total_estimated

    # Among the estimated ones, count true successes/failures.
    # (If hit has None/NaN for some reason, drop those too.)
    eval_df = estimated_df.dropna(subset=['hit'])
    eval_df['hit'] = eval_df['hit'].astype(bool)

    successful = eval_df['hit'].sum()
    failed = len(eval_df) - successful

    all_devs = eval_df['deviation'].dropna()
    failed_devs = eval_df.loc[~eval_df['hit'], 'deviation'].dropna()

    print("📊 Validation Summary")
    print("---------------------")
    print(f"Total tablets: {total}")
    print(f"  📌 With an estimate: {total_estimated}")
    print(f"  🚫 Could not be estimated: {not_estimated}")
    print()
    print(f"✅ Successful estimates: {successful}")
    print(f"❌ Failed estimates: {failed}")
    print()

    # Success / fail rates that only consider tablets that *were* estimated
    if successful + failed > 0:
        print(f"🎯 Success rate (of estimated): {successful / (successful + failed):.2%}")
        print(f"💥 Failure rate (of estimated): {failed / (successful + failed):.2%}")
    else:
        print("🎯 Success / failure rate (of estimated): N/A (no estimates produced)")

    # Optional: success rate relative to all tablets
    print(f"🌍 Success rate (of all tablets): {successful / total:.2%}")

    if len(all_devs) > 0:
        print(f"📉 Mean deviation (all successful/failed estimates): {all_devs.mean():.2f}")
        print(f"📊 Std. deviation (all successful/failed estimates): {all_devs.std():.2f}")
    else:
        print("📉 No deviation stats (no valid deviations).")

    if len(failed_devs) > 0:
        print(f"🔥 Mean deviation (failures): {failed_devs.mean():.2f}")
        print(f"🚨 Std. deviation (failures): {failed_devs.std():.2f}")
    else:
        print("🔥 No deviation stats for failures (either no failures or no deviations).")

def validate_known_tablets(df, max_active_years=17, tolerance_years=1):
    validation_results = []
    fully_dated_df = get_fully_dated_rows_by_julian(df)
    fully_dated_df['Split_Julian_dates'] = fully_dated_df['Split_Julian_dates'].astype(int)

    for tab_id, tablet in tqdm(fully_dated_df.groupby('Tablet ID'), desc="Validating tablets"):
        # Remove this tablet
        remaining_df = fully_dated_df[fully_dated_df['Tablet ID'] != tab_id].copy()

        # Build yellow pages from remaining data
        yellow_pages = YellowPages(remaining_df)

        # Estimate date
        estimated_from, estimated_to = estimate_date(
            tablet,
            yellow_pages,
            max_active_years=max_active_years
        )
        actual_year = -tablet['Split_Julian_dates'].iloc[0]  # stored as negative

        if estimated_from is not None and estimated_to is not None:
            # allow ±1 (tolerance_years) outside the range to still count as a hit
            lower = estimated_from - tolerance_years
            upper = estimated_to + tolerance_years
            hit = lower <= actual_year <= upper

            coe = (estimated_from + estimated_to) / 2
            deviation = abs(actual_year - coe)
        else:
            # No estimate produced – mark as non-estimated, not as a failure
            hit = None   # instead of False
            coe = None
            deviation = None

        validation_results.append({
            'Tablet ID': tab_id,
            'actual_year': actual_year,
            'estimated_from': estimated_from,
            'estimated_to': estimated_to,
            'coe': coe,
            'deviation': deviation,
            'hit': hit
        })

    return pd.DataFrame(validation_results)

def validate():
    df = pd.read_csv(preprocessed_prosobab_data).copy()
    df['Julian date'] = df['Julian date'].fillna('-')
    df['Split_Julian_dates'] = df['Julian date'].str.split(pat="/").str[0]
    val_results = validate_known_tablets(df)
    val_results.to_csv("../data/output/validation_results_after_correction_17.csv", index=False)
    val_results = pd.read_csv("../data/output/validation_results_after_correction_17.csv")
    summarize_validation_results(val_results)