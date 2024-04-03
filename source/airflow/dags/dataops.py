import pandas as pd


def set_default_pandas_options(
    max_columns=10, max_rows=2000, width=1000, max_colwidth=50
):

    pd.set_option("display.max_columns", max_columns)
    pd.set_option("display.max_rows", max_rows)
    pd.set_option("display.width", width)
    pd.set_option("max_colwidth", max_colwidth)


def data_quality_report_numerical(df):
    if isinstance(df, pd.core.frame.DataFrame):

        descriptive_statistics = df.describe()
        numeric_df = df.select_dtypes(exclude=["object"])
        data_types = pd.DataFrame(numeric_df.dtypes, columns=["Data Type"]).transpose()
        missing_value_counts = pd.DataFrame(
            numeric_df.isnull().sum(), columns=["Missing Values"]
        ).transpose()
        present_value_counts = pd.DataFrame(
            numeric_df.count(), columns=["Present Values"]
        ).transpose()
        data_report = pd.concat(
            [
                descriptive_statistics,
                data_types,
                missing_value_counts,
                present_value_counts,
            ],
            axis=0,
        )

        return data_report

    else:

        return None


def data_quality_report_categorical(df):
    if isinstance(df, pd.core.frame.DataFrame):
        categorical_df = df.select_dtypes(include="object")
        descriptive_statistics = categorical_df.describe(include="object")
        missing_value_counts = pd.DataFrame(
            categorical_df.isnull().sum(), columns=["Missing Values"]
        ).transpose()
        present_value_counts = pd.DataFrame(
            categorical_df.count(), columns=["Present Values"]
        ).transpose()
        data_report = pd.concat(
            [
                descriptive_statistics,
                missing_value_counts,
                present_value_counts,
            ],
            axis=0,
        )

        results_row = pd.Series(name="Mode Frequency", dtype=float)
        for column in data_report.columns:
            results_row[column] = (
                data_report.loc["freq", column]
                / data_report.loc["Present Values", column]
            )

        data_report = data_report.append(results_row)

        return data_report

    else:

        return None
