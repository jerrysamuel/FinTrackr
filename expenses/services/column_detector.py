import pandas as pd
import re
from typing import Optional, Dict


class ColumnDetector:
    """
    Detects likely date/amount/description columns in a pandas DataFrame.
    Returns a mapping suitable for df.rename(columns=mapping) where keys are
    original column names and values are standardized names: 'date','amount','description'.
    """

    DATE_PATTERNS = [
        'date', 'trans_date', 'transaction_date', 'posted_date',
        'value_date', 'timestamp', 'datetime'
    ]

    AMOUNT_PATTERNS = [
        'amount', 'value', 'debit', 'credit', 'transaction_amount',
        'sum', 'total', 'price', 'amt'
    ]

    DESCRIPTION_PATTERNS = [
        'description', 'memo', 'details', 'narrative', 'particulars',
        'transaction_details', 'remarks', 'merchant', 'narr'
    ]

    @staticmethod
    def detect_columns(df: pd.DataFrame) -> Optional[Dict[str, str]]:
        """
        Return mapping: {original_column_name: standardized_name}
        e.g. {"Transaction Date": "date", "Amt (NGN)": "amount", "Narration": "description"}
        If detection fails for required columns, returns None.
        """
        if df is None or df.columns.empty:
            return None

        # Build normalized -> original mapping for lookup
        normalized_to_original = {
            col.lower().strip(): col for col in df.columns
        }

        mapping = {}

        # Helper to find first matching column given patterns & validator fn
        def find_column(patterns, validator=None):
            # 1) exact pattern substring match
            for norm, orig in normalized_to_original.items():
                if any(p in norm for p in patterns):
                    # If validator passed, check sample values
                    if validator is None or validator(df[orig]):
                        return orig

            # 2) fallback: try columns that look like the right dtype (no name match)
            if validator:
                for orig in df.columns:
                    try:
                        if validator(df[orig]):
                            return orig
                    except Exception:
                        continue
            return None

        # validators
        def is_date_series(series):
            # try a few non-null samples to see if they parse as dates
            sample = series.dropna().astype(str).head(10)
            if sample.empty:
                return False
            try:
                parsed = pd.to_datetime(sample, errors='coerce', dayfirst=False)
                if parsed.notna().sum() >= max(1, len(sample) // 2):
                    return True
                # try dayfirst if ambiguous
                parsed2 = pd.to_datetime(sample, errors='coerce', dayfirst=True)
                return parsed2.notna().sum() >= max(1, len(sample) // 2)
            except Exception:
                return False

        def is_numeric_series(series):
            sample = series.dropna().astype(str).head(10)
            if sample.empty:
                return False
            # strip currency symbols and common noise, then try numeric
            cleaned = sample.str.replace(r'[^\d\.\-\(\)CRcrDRdr,]', '', regex=True)
            # if after cleaning majority can be parsed to float, consider numeric
            parsed = pd.to_numeric(cleaned.str.replace(',', ''), errors='coerce')
            return parsed.notna().sum() >= max(1, len(parsed) // 2)

        # find date
        date_col = find_column(ColumnDetector.DATE_PATTERNS, validator=is_date_series)
        if date_col:
            mapping[date_col] = 'date'

        # find amount
        amount_col = find_column(ColumnDetector.AMOUNT_PATTERNS, validator=is_numeric_series)
        if amount_col:
            mapping[amount_col] = 'amount'

        # find description
        desc_col = find_column(ColumnDetector.DESCRIPTION_PATTERNS, validator=None)
        if desc_col:
            mapping[desc_col] = 'description'
        else:
            # fallback: first object dtype column not used yet
            for col in df.columns:
                if col in mapping:
                    continue
                if df[col].dtype == 'object' or df[col].dtype.name.startswith('string'):
                    mapping[col] = 'description'
                    break

        # Ensure all three detected
        if set(mapping.values()) == {'date', 'amount', 'description'}:
            return mapping
        return None
