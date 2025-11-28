import pandas as pd
import re
import logging
from typing import Tuple, List, Dict, Any
from .column_detector import ColumnDetector

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ETLService:
    """Extract, Transform, Load service for transaction files."""

    @staticmethod
    def extract(file) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Read an uploaded file into a DataFrame and auto-detect columns.
        Returns (df, mapping) where mapping is suitable for df.rename(columns=mapping).
        Raises ValueError on failure with explanatory message.
        """
        try:
            # Try to read CSV using common encodings; for Excel use pandas default
            fname = getattr(file, 'name', '')
            if fname.lower().endswith('.csv'):
                try:
                    df = pd.read_csv(file, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    df = pd.read_csv(file, encoding='latin1')
            elif fname.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file)
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel.")

            if df is None or df.empty:
                raise ValueError("Uploaded file is empty or unreadable.")

            mapping = ColumnDetector.detect_columns(df)
            if not mapping:
                raise ValueError(
                    "Could not auto-detect date/amount/description columns. "
                    "Ensure your file contains those fields or rename headers."
                )
            return df, mapping

        except Exception as exc:
            logger.exception("Extraction failed")
            raise ValueError(f"File extraction failed: {str(exc)}")

    @staticmethod
    def transform(df: pd.DataFrame, column_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Transform the DataFrame into cleaned transactions.
        column_mapping should be {original_name: 'date'|'amount'|'description'}
        Returns list of dicts ready for loading.
        """
        try:
            # rename based on mapping (original -> standardized)
            df = df.rename(columns=column_mapping)

            # Ensure canonical column names are present
            required = ['date', 'amount', 'description']
            missing = [c for c in required if c not in df.columns]
            if missing:
                raise ValueError(f"Missing required columns after rename: {missing}")

            # Keep only required columns (preserve order)
            df = df[required].copy()

            # Drop rows where all required are null/empty
            df = df.dropna(how='all', subset=required)

            # Drop exact duplicates
            df = df.drop_duplicates()

            # Clean each column
            df = ETLService._clean_dates(df)
            df = ETLService._clean_amounts(df)
            df = ETLService._clean_descriptions(df)
            df = ETLService._classify_transactions(df)

            # Final: remove any rows missing critical fields
            df = df.dropna(subset=['date', 'amount', 'description'])

            if df.empty:
                raise ValueError("No valid transactions found after cleaning.")

            transactions = df.to_dict('records')
            return transactions

        except Exception as exc:
            logger.exception("Transformation failed")
            raise ValueError(f"Data transformation failed: {str(exc)}")

    @staticmethod
    def _clean_dates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Robust date parsing:
        - Try default pandas parse
        - If many NaT, try dayfirst toggle
        - Accept Excel serial numbers as dates
        - Normalize to ISO date string YYYY-MM-DD
        """
        if 'date' not in df.columns:
            raise ValueError("Date column not found")

        series = df['date']

        # If numeric-like (Excel serial), try converting
        if pd.api.types.is_numeric_dtype(series):
            try:
                df['date'] = pd.to_datetime(series, unit='d', origin='1899-12-30', errors='coerce')
            except Exception:
                df['date'] = pd.to_datetime(series, errors='coerce')
        else:
            # try parse with default (dayfirst=False)
            parsed = pd.to_datetime(series, errors='coerce', dayfirst=False)
            nat_count = parsed.isna().sum()
            total = len(parsed)
            # if many are NaT, try dayfirst=True
            if nat_count > max(1, total // 3):
                parsed2 = pd.to_datetime(series, errors='coerce', dayfirst=True)
                # choose whichever gives more parsed values
                parsed = parsed2 if parsed2.notna().sum() > parsed.notna().sum() else parsed
            df['date'] = parsed

        # Remove rows where date couldn't be parsed
        before = len(df)
        df = df.dropna(subset=['date'])
        after = len(df)
        removed = before - after
        if removed > 0:
            logger.warning("Removed %d rows due to invalid dates", removed)

        # Normalize to YYYY-MM-DD string
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        return df

    @staticmethod
    def _clean_amounts(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse amounts robustly:
        - Remove currency symbols and codes
        - Handle parentheses for negatives
        - Handle trailing CR/DR or leading CR/DR tokens
        - Remove thousands separators
        - Convert to absolute value and leave sign info for classification
        """
        if 'amount' not in df.columns:
            raise ValueError("Amount column not found")

        def parse_amount(value):
            if pd.isna(value):
                return None

            s = str(value).strip()

            # Common noise: currency codes/symbols at start or end (NGN, N, ₦, $, GBP, etc.)
            # Remove currency symbols and letters except CR/DR
            # Keep parentheses and minus sign
            # Normalize minus signs
            s = s.replace('\u2212', '-')  # minus sign
            # detect and strip common currency symbols and letters
            s = re.sub(r'(?i)[A-Z]{2,3}\b', '', s)  # currency codes like NGN, USD
            s = re.sub(r'[£$€¥₦₪₹]', '', s)
            # remove spaces
            s = s.replace(' ', '')

            # detect CR/DR at either end
            sign = 1
            if re.search(r'(?i)CR\b', s):
                sign = 1
                s = re.sub(r'(?i)CR\b', '', s)
            elif re.search(r'(?i)DR\b', s):
                sign = -1
                s = re.sub(r'(?i)DR\b', '', s)

            # parentheses => negative
            if s.startswith('(') and s.endswith(')'):
                sign = -1
                s = s[1:-1]

            # handle leading plus/minus
            if s.startswith('-'):
                sign = -1
                s = s[1:]
            elif s.startswith('+'):
                s = s[1:]

            # remove thousands separators (commas)
            s = s.replace(',', '')

            # final cleanup: remove any leftover non-numeric except dot
            s = re.sub(r'[^0-9.\-]', '', s)

            if s == '' or s == '.':
                return None

            try:
                val = float(s)
                return sign * val
            except Exception:
                return None

        parsed = df['amount'].apply(parse_amount)
        before = len(df)
        df['amount'] = parsed
        df = df.dropna(subset=['amount'])
        after = len(df)
        if before - after > 0:
            logger.warning("Dropped %d rows due to unparsable amounts", before - after)

        return df

    @staticmethod
    def _clean_descriptions(df: pd.DataFrame) -> pd.DataFrame:
        if 'description' not in df.columns:
            raise ValueError("Description column not found")

        df['description'] = df['description'].fillna('Unknown Transaction')
        df['description'] = df['description'].astype(str).str.strip()
        # collapse multiple spaces
        df['description'] = df['description'].str.replace(r'\s+', ' ', regex=True)
        # truncate to reasonable length
        df['description'] = df['description'].str.slice(0, 500)
        # remove empty strings if any
        df = df[df['description'] != '']

        return df

    @staticmethod
    def _classify_transactions(df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify transactions as CREDIT or DEBIT.
        Flexible rule: positive amounts → CREDIT, negative → DEBIT.
        Then store absolute amount and a transaction_type column.
        """
        def ttype(x):
            try:
                x = float(x)
            except Exception:
                return None
            if x > 0:
                return 'CREDIT'
            elif x < 0:
                return 'DEBIT'
            else:
                return 'NEUTRAL'

        df['transaction_type'] = df['amount'].apply(ttype)
        # Convert amount to absolute for storage (keep type to know direction)
        df['amount'] = df['amount'].abs()
        # optionally drop NEUTRAL if undesired
        return df
