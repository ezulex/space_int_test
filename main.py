import pandas as pd
import json
from datetime import datetime, timedelta


def parse_json(data):
    if pd.isna(data):
        return None
    if isinstance(data, (list, dict)):
        return data
    if isinstance(data, str):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None
    return None


def parse_date(str_date, iso=0):
    if iso == 0:
        try:
            return datetime.strptime(str_date, '%d.%m.%Y').date()
        except ValueError:
            return None
    elif iso == 1:
        try:
            return datetime.fromisoformat(str_date).date()
        except ValueError:
            return None
    else:
        raise ValueError("Invalid value for 'iso'. Use 0 for DD.MM.YYYY or 1 for ISO format.")


def calculate_tot_claim_cnt_l180d(contracts, application_date, threshold=180):
    if not contracts:
        return -3

    claim_count = 0
    from_date = parse_date(application_date, 1)
    threshold_date = from_date - timedelta(days=threshold)

    for contract in contracts:
        if isinstance(contract, dict) and 'claim_id' in contract and 'claim_date' in contract and contract[
            'claim_date']:
            try:
                claim_date = parse_date(contract['claim_date'], 0)

                if claim_date >= threshold_date:
                    claim_count += 1
            except ValueError:
                continue

    if claim_count > 0:
        return claim_count
    else:
        return -3


def calculate_disb_bank_loan_wo_tbc(contracts):
    if not contracts:
        return -3

    excluded_banks = ['LIZ', 'LOM', 'MKO', 'SUG', None]
    total_exposure = 0
    loan_count = 0
    has_claims = False

    for contract in contracts:
        if isinstance(contract, dict):
            # Проверка на наличие claim_id и claim_date
            if 'claim_id' in contract and 'claim_date' in contract:
                has_claims = True

            # Проверяем условия: банк не в списке исключенных и contract_date не пустой
            if (contract.get('bank') not in excluded_banks and
                    'loan_summa' in contract and contract['loan_summa'] and
                    'contract_date' in contract and contract['contract_date']):
                try:
                    total_exposure += int(
                        contract['loan_summa'])  # May be using float, but I think that loans can't be float
                    loan_count += 1
                except ValueError:
                    continue

    if not has_claims:
        return -3  # No claims
    if loan_count == 0:
        return -1  # No loans
    if total_exposure > 0:
        return total_exposure
    else:
        return -1


def calculate_day_sinlastloan(contracts, application_date):
    if not contracts:
        return -3

    from_date = parse_date(application_date, 1)
    last_loan_date = None
    has_valid_loan = False
    has_claims = False

    for contract in contracts:
        if isinstance(contract, dict):
            if 'claim_id' in contract and 'claim_date' in contract:
                has_claims = True
            # Special notes:
            # 1. Take last loan of client where summa is not null and calculate number of days from contract_date of this loan to application date.
            # Is it right? summa, not loan_summa?
            if ('contract_date' in contract and contract['contract_date'] and
                    'summa' in contract and contract['summa']):
                try:
                    loan_summa = int(
                        contract['summa'])  # May be using float, but I think that loans can't be float
                    if loan_summa > 0:
                        has_valid_loan = True
                        contract_date = parse_date(contract['contract_date'], 0)
                        if last_loan_date is None or contract_date > last_loan_date:
                            last_loan_date = contract_date
                except ValueError:
                    continue

    if not has_claims:
        return -3  # No claims

    if not has_valid_loan:
        return -1  # No loans

    if last_loan_date:
        return (from_date - last_loan_date).days
    else:
        return -1


# Load the data
df = pd.read_csv('data.csv')

# Parse JSON
df['contracts'] = df['contracts'].apply(parse_json)

# Calculate features
df['tot_claim_cnt_l180d'] = df.apply(
    lambda row: calculate_tot_claim_cnt_l180d(row['contracts'], row['application_date'], 180), axis=1)
df['disb_bank_loan_wo_tbc'] = df['contracts'].apply(calculate_disb_bank_loan_wo_tbc)
df['day_sinlastloan'] = df.apply(lambda row: calculate_day_sinlastloan(row['contracts'], row['application_date']),
                                 axis=1)
# Select columns for output
output_columns = ['id', 'application_date', 'tot_claim_cnt_l180d', 'disb_bank_loan_wo_tbc', 'day_sinlastloan']
output_df = df[output_columns]

# Save the output to CSV
output_df.to_csv('contract_features.csv', index=False)
