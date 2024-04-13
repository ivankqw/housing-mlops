import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


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


def categorical_charts(df: pd.DataFrame, df_name: str) -> str:
    cat_cols = list(df.select_dtypes(include="object").columns)
    selected_cols = []
    for col in cat_cols:
        if df[col].nunique() <= 30:
            selected_cols.append(col)
    num_cols = 1
    num_rows = (len(selected_cols) + num_cols - 1) // num_cols
    plt.figure(figsize=(20, 5 * num_rows))
    plt.suptitle("Categorical Columns Distribution", fontsize=16, y=1.005)
    for idx, col in enumerate(selected_cols, start=1):
        plt.subplot(num_rows, num_cols, idx)
        sns.countplot(x=df[col])
        plt.title(col + " distribution")
        plt.xticks(rotation=45)

    chart_path = "../charts/cat_charts_" + df_name + ".png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    return chart_path


def numerical_charts(df: pd.DataFrame, df_name: str) -> str:
    numerical_cols = list(df.select_dtypes(exclude="object").columns)
    num_cols = 1
    num_rows = (len(numerical_cols) + num_cols - 1) // num_cols
    plt.figure(figsize=(20, 5 * num_rows))
    plt.suptitle(df_name + " Numerical Columns Distribution", fontsize=16, y=1.005)
    for idx, col in enumerate(numerical_cols, start=1):
        plt.subplot(num_rows, num_cols, idx)
        sns.histplot(x=df[col], kde=True)
        plt.title(col + " distribution")
        plt.xticks(rotation=45)
    plt.tight_layout()

    chart_path = "../charts/num_charts_" + df_name + ".png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    return chart_path
