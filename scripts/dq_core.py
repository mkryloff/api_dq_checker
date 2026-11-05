import abc
import pandas as pd

class BaseDQCheck(abc.ABC):

    def __init__(self, check_name: str):
        self.check_name = check_name

    @abc.abstractmethod
    def run(self, df: pd.DataFrame, column: str, run_id: str) -> dict:
        pass


class NullCheck(BaseDQCheck):

    def __init__(self):
        super().__init__(check_name="null_check")

    def run(self, df: pd.DataFrame, column: str, run_id: str) -> dict:
        records_scanned = len(df)

        bad_records_count = int(df[column].isnull().sum())

        if bad_records_count == 0:
            status = "SUCCESS"
            comment = f"Проверка пройдена. Ошибок не обнаружено."
        else:
            status = "FAILED"
            comment = f"Обнаружено {bad_records_count} пропущенных значений в колонке '{column}'."

        return {
            "check_name": self.check_name,
            "run_id": run_id,
            "status": status,
            "records_scanned": records_scanned,
            "bad_records_count": bad_records_count,
            "comment": comment
        }

class DuplicatesDQCheck(BaseDQCheck):

    def __init__(self, check_name: str):
        super().__init__(check_name)

    def run(self, df: pd.DataFrame, column: str, run_id: str) -> dict:
        records_scanned = len(df)

        bad_records_count = int(df[column].duplicated().sum())

        if bad_records_count == 0:
            status = "SUCCESS"
            comment = f"Проверка пройдена. Дубликатов не обнаружено."
        else:
            status = "FAILED"
            comment = f"Обнаружено {bad_records_count} дубликатов в колонке '{column}'."

        return {
            "check_name": self.check_name,
            "run_id": run_id,
            "status": status,
            "records_scanned": records_scanned,
            "bad_records_count": bad_records_count,
            "comment": comment
        }

class AnomalyDQCheck(BaseDQCheck):

    def __init__(self, threshold: float = 0.30):
        super().__init__(check_name="anomaly_check")
        self.threshold = threshold

    def run(self, df: pd.DataFrame, column: str, run_id: str) -> dict:
        records_scanned = len(df)
        bad_records_count = 0

        df_sorted = df.sort_values(by="report_date").copy()

        for currency, group in df_sorted.groupby('currency_code'):
            pct_changes = group[column].pct_change().abs()
            anomalies_in_currency = (pct_changes > self.threshold).sum()
            bad_records_count += int(anomalies_in_currency)

        if bad_records_count == 0:
            status = "SUCCESS"
            comment = f"Проверка пройдена. Критических ценовых аномалий (> {self.threshold * 100}%) не обнаружено."
        else:
            status = "FAILED"
            comment = f"Обнаружено {bad_records_count} резких скачков курса (> {self.threshold * 100}%) относительно предыдущих значений."

        return {
            "check_name": self.check_name,
            "run_id": run_id,
            "status": status,
            "records_scanned": records_scanned,
            "bad_records_count": bad_records_count,
            "comment": comment
        }