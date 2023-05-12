import pandas as pd
from glob import glob

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 500)

NUMBER_FORMATS = 3
file_names = glob('raw_data/*')


def unnamed_to_null(column_name, index=None, columns=None):
    if 'Unnamed' in column_name:
        if index:
            return previous_non_null_value(index, columns).strip()
        else:
            return ''
    return column_name.strip()


def previous_non_null_value(index, columns):
    columns_first = [i for (i, j) in columns]
    columns_first = columns_first[:index + 1]
    columns_first.reverse()
    for _ in columns_first:
        if 'Unnamed' not in _:
            return _
    return ''


def get_required_subset(df):
    COLUMN_SYNONYMS = {
        'WC name': 'coverage_name',
        'Symass Name (if available)': 'symass_name',
        'Compact (V6)Limit': 'compact_limit',
        'Comfort (V6)Limit': 'comfort_limit',
        'Comfort+ (V6)Limit': 'comfort_plus_limit',
        'Switch Tariff Limit': 'switch_limit',
        'Switch Green TariffLimit': 'switch_green_limit',
        'Green CompactLimit': 'compact_green_limit',
        'Green ComfortLimit': 'comfort_green_limit',
        'Green Comfort+Limit': 'comfort_plus_green_limit',

        'Compact (v6)Limit': 'compact_limit',
        'Comfort (v6)Limit': 'comfort_limit',
        'Comfort+ (v6)Limit': 'comfort_plus_limit',

        'Parameter Name / Coverages & Assistances': 'coverage_name',
        'Compact  - EcoLimit': 'compact_limit',
        'ComfortLimit': 'comfort_limit',
        'Switch Tarif - WechseltarifLimit': 'switch_limit',
        "what's covered, what's not": 'is_covered',

        'Insurances': 'insurance_class',
        'Group': 'insurance_class',

        'Generelle Informationen': 'coverage_name',
        # 'Compact (V.6.0)': 'compact_limit',
        'Compact v2021': 'compact_limit',
        # 'Comfort (V.6.0)': 'comfort_limit',
        'Comfort V2021': 'comfort_limit',
        # 'Comfort+ (V6.0)': 'comfort_plus_limit',
        'Comfort+ v2021': 'comfort_plus_limit',

        'Parameter Name Coverages': 'coverage_name',
        'Switch': 'switch_limit',

        'coverage': 'coverage_name',
        'Parameter Name': 'coverage_name',
        'CompactLimit': 'compact_limit',
        'Comfort+Limit': 'comfort_plus_limit',
        'Assistance - Premium': 'assistance_limit',
        'Assistance - Premium Plus': 'assistance_plus_limit',

    }
    subset_columns = [c for c in df.columns if c in COLUMN_SYNONYMS]
    df_new = df[subset_columns].copy()
    df_new.columns = df_new.columns.map(COLUMN_SYNONYMS)
    return df_new


def filter_data():
    dataset = []
    unique_columns = set(['coverage_name'])
    for file_name in file_names:
        temporary_table = pd.read_csv(file_name, header=[0, 1])
        temporary_table.columns = [unnamed_to_null(first, i, temporary_table.columns) + unnamed_to_null(second) for
                                   i, (first, second) in
                                   enumerate(temporary_table.columns)]
        temporary_table.columns = temporary_table.columns.to_series().ffill()
        temporary_table = get_required_subset(temporary_table)
        if temporary_table.empty or len(temporary_table.columns) < 6:
            print(f'{file_name} has not been included in the dataset')
        else:
            temporary_table['product_name'] = file_name.split('/')[1].split('.csv')[0]
            dataset.append(temporary_table)
            unique_columns.update(list(temporary_table.columns))
    return dataset, unique_columns


def combine_data(dataset, unique_columns):
    data = pd.DataFrame(columns=list(unique_columns))
    for i, d in enumerate(dataset):
        data = pd.concat([data, d])
    return data


def split_data_by_package(dataset):
    package_columns = [c for c in dataset.columns if '_limit' in c]
    other_columns = [c for c in dataset.columns if c not in package_columns]

    new_dataset = pd.DataFrame(columns=list(other_columns) + ['package'])
    for p in package_columns:
        data = dataset.loc[:, [*other_columns, p]]
        data.rename(columns={p: 'limit'}, inplace=True)
        data['package'] = p.replace('_limit', '')
        new_dataset = pd.concat([new_dataset, data])
    return new_dataset


def get_limit_multiple(limit):
    if '%' in limit:
        return limit
    return None


def clean_data(dataset):
    dataset_adjusted = dataset.copy()
    dataset_adjusted.is_covered = dataset_adjusted.is_covered.apply(
        lambda x: False if x == 'nein' else True if str(x).startswith('ja') else None)
    dataset_adjusted.limit = dataset_adjusted.limit.astype(str)

    dataset_adjusted.limit = (
        dataset_adjusted.limit.str.replace('€', '')
            .str.replace('keine', '')
            .str.replace('unbeschränkt', '')
            .str.replace('-', '')
            .str.replace('nan', '')
            .str.replace('% <Sum Insured>', '%')
            .str.replace('% SumInsured', '%')
            .str.replace('<Sum Insured>', '100%')
            .str.replace('<SumInsured>', '100%')
            .str.replace('<Sum Insured Bike>', '100%')
            .str.replace('.000', '000')
            .str.replace(',000', '000')
            .str.replace('.500', '500')
            .str.replace(',500', '500')
            .str.strip()
    )
    dataset_adjusted.drop(columns=['insurance_class', 'symass_name'], inplace=True)
    dataset_adjusted['country_isocode'] = dataset_adjusted.product_name.apply(lambda x: x[-2:].upper())
    dataset_adjusted.product_name = dataset_adjusted.product_name.apply(lambda x: x[:-3])
    # dataset_adjusted = dataset_adjusted.loc[dataset_adjusted.is_covered != False, :]
    return dataset_adjusted


data = combine_data(*filter_data())
data = split_data_by_package(data)
data = clean_data(data)

print(data.describe())

# TODO: require offering too eg comprehensive

