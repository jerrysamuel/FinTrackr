from groq import Groq
import pandas as pd
import re
import logging
from typing import Tuple, List, Dict, Any, Set
from .column_detector import ColumnDetector
from expenses.models import Expense
from django.db import transaction
from expenses.models import Expense
from django.utils.dateparse import parse_date
from decimal import Decimal
from openai import OpenAI
import json
from decouple import config 
from django.conf import settings


api_key = settings.APIKEY_OPENAI



logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class ETLService:

    @staticmethod
    def extract(file) -> Tuple[pd.DataFrame, Dict[str, str]]:
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
    def transform(df: pd.DataFrame, column_mapping: Dict[str, str], api_key: str = None) -> Tuple[List[Dict[str, Any]], Set[str]]:
        try:
            # rename based on mapping (original -> standardized)
            df = df.rename(columns=column_mapping)

            # Look for possible category column if not already mapped
            optional = ['category']
            columns_to_keep = ['date', 'amount', 'description']
            for col in list(df.columns):
                if col.lower() in ['category', 'type', 'group', 'class', 'cat'] and col not in columns_to_keep:
                    df = df.rename(columns={col: 'category'})
                    columns_to_keep.append('category')
                    break

            # Ensure canonical column names are present
            required = ['date', 'amount', 'description']
            missing = [c for c in required if c not in df.columns]
            if missing:
                raise ValueError(f"Missing required columns after rename: {missing}")

            # Keep only required + optional columns (preserve order)
            df = df[columns_to_keep].copy()

            # Drop rows where all required are null/empty
            df = df.dropna(how='all', subset=required)

            # Drop exact duplicates
            df = df.drop_duplicates()

            # Clean each column
            df = ETLService._clean_dates(df)
            df = ETLService._clean_amounts(df)
            df = ETLService._clean_descriptions(df)

            # Infer categories using LLM if no category column and api_key provided
            if 'category' not in df.columns:
                if api_key:
                    df = ETLService._infer_categories(df, api_key)
                else:
                    df['category'] = 'Uncategorized'

            if 'category' in df.columns:
                df = ETLService._clean_categories(df)

            df = ETLService._classify_transactions(df)

            # Final: remove any rows missing critical fields
            df = df.dropna(subset=['date', 'amount', 'description'])
            if df.empty:
                raise ValueError("No valid transactions found after cleaning.")

            # Extract unique categories if present
            unique_categories = set(df['category'].dropna().unique()) if 'category' in df.columns else set()

            transactions = df.to_dict('records')
            return transactions, unique_categories
        except Exception as exc:
            logger.exception("Transformation failed")
            raise ValueError(f"Data transformation failed: {str(exc)}")

    @staticmethod
    def _infer_categories(df: pd.DataFrame, api_key: str) -> pd.DataFrame:
        """Use Groq's Llama model to infer categories from descriptions."""
        
        if not api_key or api_key.strip() == "":
            logger.warning("No API key provided. Using 'Uncategorized' for all transactions.")
            df['category'] = 'Uncategorized'
            return df
        
        try:
            client = OpenAI(
                api_key=api_key,
                base_url="https://api.groq.com/openai/v1"
            )
            
            descriptions = df['description'].tolist()
            logger.info(f"Sending {len(descriptions)} descriptions to Groq API...")
            
            # More explicit prompt with numbered format
            prompt = (
                f"You must categorize EXACTLY {len(descriptions)} transaction descriptions.\n\n"
                "Categories to use:\n"
                "- Food & Dining\n"
                "- Transportation\n"
                "- Shopping\n"
                "- Bills & Utilities\n"
                "- Entertainment\n"
                "- Healthcare\n"
                "- Travel\n"
                "- Education\n"
                "- Personal Care\n"
                "- Groceries\n"
                "- Income\n"
                "- Miscellaneous\n\n"
                f"Return a JSON object with EXACTLY {len(descriptions)} categories in the 'categories' array.\n"
                "Each transaction gets ONE category. Do not skip any.\n\n"
                "Transactions:\n" + 
                "\n".join([f"{i+1}. {desc}" for i, desc in enumerate(descriptions)]) +
                f"\n\nRemember: Return EXACTLY {len(descriptions)} categories in order."
            )
            
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a financial categorization expert. You MUST return valid JSON with the exact number of categories requested. Never skip transactions."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Even lower for more consistent output
                max_tokens=3000,
            )
            
            content = response.choices[0].message.content.strip()
            logger.info(f"API Response received: {len(content)} characters")
            
            # Remove markdown code blocks if present
            import re
            # Try to extract JSON from markdown
            json_match = re.search(r'```(?:json)?\s*(.*?)\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1).strip()
                logger.info("Extracted JSON from markdown blocks")
            
            # Parse JSON
            try:
                parsed = json.loads(content)
                categories = parsed.get("categories", [])
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse failed: {e}")
                logger.error(f"Content: {content[:500]}")
                # Last resort: try to find array in content
                array_match = re.search(r'"categories"\s*:\s*\[(.*?)\]', content, re.DOTALL)
                if array_match:
                    array_content = '[' + array_match.group(1) + ']'
                    categories = json.loads(array_content)
                else:
                    raise ValueError(f"Could not parse categories from response")
            
            logger.info(f"Parsed {len(categories)} categories from API")
            
            # Handle mismatch
            if len(categories) != len(descriptions):
                logger.warning(f"Category count mismatch: got {len(categories)}, expected {len(descriptions)}")
                
                # If we got fewer categories, pad with 'Miscellaneous'
                if len(categories) < len(descriptions):
                    missing = len(descriptions) - len(categories)
                    logger.warning(f"Padding {missing} missing categories with 'Miscellaneous'")
                    categories.extend(['Miscellaneous'] * missing)
                
                # If we got more, truncate
                elif len(categories) > len(descriptions):
                    logger.warning(f"Truncating {len(categories) - len(descriptions)} extra categories")
                    categories = categories[:len(descriptions)]
            
            df['category'] = categories
            logger.info(f"✓ Successfully categorized {len(categories)} transactions")
            return df
            
        except Exception as exc:
            logger.error(f"Groq API failed: {exc}", exc_info=True)
            logger.warning("Falling back to 'Uncategorized' for all transactions")
            df['category'] = 'Uncategorized'
            return df

    @staticmethod
    def _clean_dates(df: pd.DataFrame) -> pd.DataFrame:
        """ Robust date parsing:
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
    def _clean_categories(df: pd.DataFrame) -> pd.DataFrame:
        if 'category' not in df.columns:
            return df
        df['category'] = df['category'].fillna('Uncategorized')
        df['category'] = df['category'].astype(str).str.strip().str.title()
        return df

    @staticmethod
    def _classify_transactions(df: pd.DataFrame) -> pd.DataFrame:
        """ Classify transactions as CREDIT or DEBIT.
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
    
    @staticmethod
    def load(transactions: List[Dict[str, Any]], user) -> int:
       
        if not transactions:
            return 0
        
        from expenses.models import Category
        
        print(f"\n=== LOAD START ===")
        print(f"Loading {len(transactions)} transactions for user: {user}")
        
        # Step 1: Collect all unique category names
        category_names = set()
        for tx in transactions:
            cat_name = tx.get('category')
            if cat_name and cat_name.strip() and cat_name != 'Uncategorized':
                category_names.add(cat_name.strip())
        
        print(f"Unique categories found: {category_names}")
        
        # Step 2: Get or create user-specific categories
        category_map = {}  # Map category name -> Category object
        
        for cat_name in category_names:
            try:
                # Try to get existing category for this user
                category_obj = Category.objects.filter(
                    user=user,
                    name=cat_name
                ).first()
                
                if category_obj:
                    print(f"  existing: {cat_name} (id={category_obj.id})")
                else:
                    # Create new user-specific category
                    category_obj = Category.objects.create(
                        user=user,
                        name=cat_name,
                        is_default=False
                    )
                    print(f"  created: {cat_name} (id={category_obj.id})")
                
                category_map[cat_name] = category_obj
                
            except Exception as e:
                logger.error(f"Error creating category '{cat_name}': {e}")
                print(f"  error: {cat_name} - {e}")
        
        print(f"Total categories mapped: {len(category_map)}")
        
        # Step 3: Create expense objects
        objs = []
        skipped = 0
        
        for i, tx in enumerate(transactions):
            try:
                category_name = tx.get('category', '').strip()
                category_obj = None
                
                if category_name and category_name != 'Uncategorized':
                    category_obj = category_map.get(category_name)
                
                if i < 3:  # Debug first 3
                    print(f"\nTransaction {i+1}:")
                    print(f"  Description: {tx['description'][:40]}")
                    print(f"  Category name: '{category_name}'")
                    print(f"  Category object: {category_obj}")
                
                expense = Expense(
                    user=user,
                    date=parse_date(tx["date"]),
                    amount=Decimal(str(tx["amount"])),
                    description=tx["description"],
                    transaction_type=tx["transaction_type"],
                    category=category_obj,  # Can be None for uncategorized
                    notes="",
                )
                objs.append(expense)
                
            except Exception as e:
                skipped += 1
                logger.error(f"SKIPPED ROW {i}: {tx.get('description', 'N/A')}, ERROR: {e}")
                import traceback
                print(f"  Error on transaction {i}: {traceback.format_exc()}")
        
        if skipped > 0:
            print(f"\n⚠ Skipped {skipped} transactions due to errors")
        
        # Step 4: Bulk create all expenses
        print(f"\nBulk creating {len(objs)} expense objects...")
        
        try:
            created = Expense.objects.bulk_create(objs)
            print(f"✓ Successfully created {len(created)} expenses")
        except Exception as e:
            logger.error(f"Bulk create failed: {e}")
            import traceback
            print(f"Bulk create error: {traceback.format_exc()}")
            return 0
        
        # Step 5: Verify what was saved
        print(f"\nVerifying saved data...")
        sample = Expense.objects.filter(user=user).select_related('category').order_by('-id')[:5]
        print(f"Sample of last 5 expenses:")
        for exp in sample:
            cat_name = exp.category.name if exp.category else "None"
            print(f"  {exp.description[:40]:40} -> {cat_name}")
        
        # Check category distribution
        from django.db.models import Count
        cat_stats = Expense.objects.filter(user=user).values('category__name').annotate(count=Count('id'))
        print(f"\nCategory distribution:")
        for stat in cat_stats[:10]:
            cat = stat['category__name'] or 'Uncategorized'
            print(f"  {cat}: {stat['count']} expenses")
        
        print(f"=== LOAD END ===\n")
        
        return len(created)
            