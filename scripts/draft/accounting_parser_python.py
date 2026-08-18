import re
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging
from pathlib import Path
import sys
import argparse
import boto3
from botocore.client import Config
from minio import Minio
from minio.error import S3Error
import tempfile
import os
import csv
from datetime import datetime, timedelta
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GLParser:
    """Parser for General Ledger text files"""
    
    def __init__(self):
        # Pattern for account code (AC. or MD.)
        self.account_code_pattern = re.compile(r'^\s+(AC\.|MD\.)')
        
        # Pattern for continuation line (starts with spaces followed by continuation text)
        # Matches: .....TL0010002, ..TL0010002, TL0010002, 0010002, L0010002, etc.
        self.continuation_pattern = re.compile(r'^\s+[.\dTLA-Z]+[\w.]*$')
        
        # Pattern for data row with all 4 numbers
        self.data_row_pattern = re.compile(
            r'^\s*(\d{4})?\s+(.+?)\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s*$'
        )
        
        # Pattern for account code with data on same line
        self.account_with_data_pattern = re.compile(
            r'^\s+((?:AC|MD)\.[^\s]+)\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
            r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s*$'
        )
        
        # Pattern for header row (line code + description, no numbers)
        self.header_pattern = re.compile(
            r'^\s*(\d{4})\s+([A-Za-z\s].+?)\s*$'
        )
        
        # Pattern for description-only lines 
        # Matches: "   L0010002  (NewArrngmt Fee)", "   xp)", "   (Int Exp)", "   t Income 2)", "   State Charge)", 
        #          "   itial Fee - C)", "   wer, Light &)", "   el and Lubric)", "   curity/Janito)", 
        #          "   2  (Int Income 2)", "   (Int Income 2)", etc.
        # These are description fragments that may contain letters, numbers, spaces, parentheses, hyphens, commas, etc.
        self.description_only_pattern = re.compile(
            r'^\s+(?:[A-Z0-9]+\s+)?\([^)]*\)?\s*$|^\s+[a-zA-Z][a-zA-Z\s\-,&./]*\)\s*$'
        )
        
        self.pending_account_code = None
        self.last_row_was_data = False
        self.last_result = None  # Track last result to append continuation
        self.results = []
        self.stats = {
            'total_lines': 0,
            'data_rows': 0,
            'header_rows': 0,
            'skipped_rows': 0,
            'account_codes': 0,
            'continuation_lines': 0,
            'warnings': []
        }
    
    def clean_number(self, value: str) -> Optional[float]:
        """Convert string number to float"""
        if not value or value.strip() == "":
            return None
        try:
            return float(value.replace(',', ''))
        except Exception as e:
            logger.warning(f"Failed to parse number '{value}': {e}")
            return None
    
    def is_page_header(self, line: str) -> bool:
        """Check if line is page header (to skip)"""
        skip_patterns = [
            'RE000010', 'AREA NAME', 'PAGE NO',
            'LINE  DESCRIPTION', '----  -----------',
            'PRINTED AT', '( IN FULL AMOUNT )',
            'OPENING BALANCE', 'DEBIT MOVEMENTS'
        ]
        return any(pattern in line for pattern in skip_patterns)
    
    def is_end_marker(self, line: str) -> bool:
        """Check if line is end of report"""
        return 'END OF REPORT' in line
    
    def parse_line(self, line: str, line_num: int) -> Optional[Dict]:
        """Parse a single line and return structured data"""
        
        # Skip empty lines
        if not line.strip():
            self.last_row_was_data = False
            return None
        
        # Skip page headers
        if self.is_page_header(line):
            self.last_row_was_data = False
            return None
        
        # Check for description-only line (comes after data rows)
        if self.description_only_pattern.match(line):
            # This is a description that comes after a data row, we can skip it
            # or optionally append it to the last row
            logger.debug(f"Line {line_num}: Description-only line - {line.strip()}")
            return None
        
        # Check for continuation line (continues previous account code)
        if self.continuation_pattern.match(line):
            continuation = line.strip()
            if self.pending_account_code:
                # Append to pending account code
                self.pending_account_code += continuation
                self.stats['continuation_lines'] += 1
                logger.debug(f"Line {line_num}: Continuation (pending) - {continuation}")
            elif self.last_result and self.last_row_was_data:
                # Append to last result's description (account code that already had data)
                self.last_result['description'] += continuation
                self.stats['continuation_lines'] += 1
                logger.debug(f"Line {line_num}: Continuation (last result) - {continuation}")
            else:
                logger.warning(f"Line {line_num}: Orphan continuation: {continuation}")
                self.stats['warnings'].append(f"Line {line_num}: Orphan continuation")
            return None
        
        # Check for account code with data on same line
        account_data_match = self.account_with_data_pattern.match(line)
        if account_data_match:
            account_code = account_data_match.group(1).strip()
            
            result = {
                'line': '',
                'description': account_code,
                'opening_balance': self.clean_number(account_data_match.group(2)),
                'debit_movements': self.clean_number(account_data_match.group(3)),
                'credit_movements': self.clean_number(account_data_match.group(4)),
                'closing_balance': self.clean_number(account_data_match.group(5))
            }
            self.stats['data_rows'] += 1
            self.stats['account_codes'] += 1
            self.last_row_was_data = True
            self.last_result = result
            logger.debug(f"Line {line_num}: Account with data - {account_code}")
            return result
        
        # Check for account code (without data)
        if self.account_code_pattern.match(line):
            self.pending_account_code = line.strip()
            self.stats['account_codes'] += 1
            self.last_row_was_data = False
            logger.debug(f"Line {line_num}: Account code start - {self.pending_account_code}")
            return None
        
        # Try to match data row with numbers
        data_match = self.data_row_pattern.match(line)
        if data_match:
            line_code = data_match.group(1) if data_match.group(1) else ""
            description = data_match.group(2).strip()
            
            # Add pending account code to description
            if self.pending_account_code:
                description = f"{self.pending_account_code} {description}"
                logger.debug(f"Line {line_num}: Added account code to description")
                self.pending_account_code = None
            
            result = {
                'line': line_code,
                'description': description,
                'opening_balance': self.clean_number(data_match.group(3)),
                'debit_movements': self.clean_number(data_match.group(4)),
                'credit_movements': self.clean_number(data_match.group(5)),
                'closing_balance': self.clean_number(data_match.group(6))
            }
            self.stats['data_rows'] += 1
            self.last_row_was_data = True
            self.last_result = result
            logger.debug(f"Line {line_num}: Data row - Line {line_code}")
            return result
        
        # Try to match header row (no numbers)
        header_match = self.header_pattern.match(line)
        if header_match:
            result = {
                'line': header_match.group(1),
                'description': header_match.group(2).strip(),
                'opening_balance': None,
                'debit_movements': None,
                'credit_movements': None,
                'closing_balance': None
            }
            self.stats['header_rows'] += 1
            self.last_row_was_data = False
            self.last_result = result
            logger.debug(f"Line {line_num}: Header row - {result['line']} {result['description']}")
            return result
        
        # Line not recognized
        if line.strip():  # Only warn for non-empty lines
            logger.warning(f"Line {line_num}: Unrecognized - {line[:80]}")
            self.stats['warnings'].append(f"Line {line_num}: Unrecognized")
            self.stats['skipped_rows'] += 1
        
        self.last_row_was_data = False
        return None
    
    def parse_file(self, filepath: str) -> pd.DataFrame:
        """Parse entire GL file"""
        
        logger.info(f"Starting to parse file: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        self.stats['total_lines'] = len(lines)
        logger.info(f"Total lines in file: {len(lines)}")
        
        for i, line in enumerate(lines, 1):
            # Check for end marker
            if self.is_end_marker(line):
                logger.info(f"End of report found at line {i}")
                break
            
            result = self.parse_line(line, i)
            if result:
                self.results.append(result)
        
        # Create DataFrame
        df = pd.DataFrame(self.results)
        
        # Add order_id column as the first column with sequential numbers
        if not df.empty:
            df.insert(0, 'order_id', range(1, len(df) + 1))
        
        # Print statistics
        self.print_statistics(df)
        
        return df
    
    def print_statistics(self, df: pd.DataFrame):
        """Print parsing statistics"""
        logger.info("=" * 80)
        logger.info("PARSING STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Total lines in file:       {self.stats['total_lines']:,}")
        logger.info(f"Data rows extracted:       {self.stats['data_rows']:,}")
        logger.info(f"Header rows extracted:     {self.stats['header_rows']:,}")
        logger.info(f"Account codes found:       {self.stats['account_codes']:,}")
        logger.info(f"Continuation lines:        {self.stats['continuation_lines']:,}")
        logger.info(f"Skipped rows:              {self.stats['skipped_rows']:,}")
        logger.info(f"Total rows in output:      {len(df):,}")
        logger.info(f"Warnings:                  {len(self.stats['warnings'])}")
        logger.info("=" * 80)
        
        if self.stats['warnings'] and len(self.stats['warnings']) <= 20:
            logger.info("\nWarnings:")
            for warning in self.stats['warnings']:
                logger.info(f"  - {warning}")
        elif len(self.stats['warnings']) > 20:
            logger.info(f"\nShowing first 20 of {len(self.stats['warnings'])} warnings:")
            for warning in self.stats['warnings'][:20]:
                logger.info(f"  - {warning}")
    
    def validate_balance_equation(self, df: pd.DataFrame) -> List[str]:
        """
        Validate balance equation for rows with data
        Formula: Closing = Opening + Debit - Credit
        """
        errors = []
        
        for idx, row in df.iterrows():
            if pd.notna(row['opening_balance']) and pd.notna(row['closing_balance']):
                opening = row['opening_balance'] or 0
                debit = row['debit_movements'] or 0
                credit = row['credit_movements'] or 0
                closing = row['closing_balance'] or 0
                
                # Try different formulas (accounting conventions vary)
                # Formula 1: Closing = Opening + Debit - Credit
                calculated1 = opening + debit - credit
                
                # Formula 2: Closing = Opening - Debit + Credit
                calculated2 = opening - debit + credit
                
                tolerance = 0.01  # 1 cent tolerance
                
                if abs(calculated1 - closing) <= tolerance:
                    continue  # Formula 1 works
                elif abs(calculated2 - closing) <= tolerance:
                    continue  # Formula 2 works
                else:
                    errors.append(
                        f"Line {row['line']}: Balance mismatch. "
                        f"Opening={opening:.2f}, Debit={debit:.2f}, Credit={credit:.2f}, "
                        f"Closing={closing:.2f} (Expected {calculated1:.2f} or {calculated2:.2f})"
                    )
        
        return errors
    
    def calculate_totals(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate totals for validation"""
        if df.empty or len(df) == 0:
            return {
                'opening_balance': 0.0,
                'debit_movements': 0.0,
                'credit_movements': 0.0,
                'closing_balance': 0.0
            }
        
        # Check if required columns exist
        required_columns = ['opening_balance', 'debit_movements', 'credit_movements', 'closing_balance']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns in DataFrame: {missing_columns}")
            return {
                'opening_balance': 0.0,
                'debit_movements': 0.0,
                'credit_movements': 0.0,
                'closing_balance': 0.0
            }
        
        totals = {
            'opening_balance': df['opening_balance'].fillna(0).sum(),
            'debit_movements': df['debit_movements'].fillna(0).sum(),
            'credit_movements': df['credit_movements'].fillna(0).sum(),
            'closing_balance': df['closing_balance'].fillna(0).sum()
        }
        return totals


def extract_branch_name(input_file_path: str) -> str:
    """
    Extract branch identifier from the first line containing RE[digits] pattern
    Returns full branch identifier (e.g., "RE000010_T24_UPGRADE", "RE000020_LIVE_SYSTEM", etc.)
    Uses flexible RE pattern matching - supports RE000010, RE000020, etc.
    """
    file_path = Path(input_file_path)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read lines until we find the one with RE[digits] pattern
            line_num = 0
            for line in f:
                line_num += 1
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                logger.debug(f"Line {line_num}: {line}")
                
                # Look for the line with RE followed by digits (RE000010, RE000020, etc.)
                re_pattern = re.compile(r'^RE\d+')
                if re_pattern.match(line):
                    logger.debug(f"Found RE[digits] line: {line}")
                    
                    # Split by spaces
                    parts = line.split()
                    if len(parts) >= 2:
                        # Find index of BNCTL to determine where branch name ends
                        try:
                            bnctl_index = next(i for i, part in enumerate(parts) if 'BNCTL' in part)
                            # Extract full branch identifier from RE[digits] to BNCTL
                            branch_parts = parts[0:bnctl_index]
                            branch_identifier = '_'.join(branch_parts)
                            # Clean branch identifier - replace special characters
                            branch_identifier = re.sub(r'[^A-Za-z0-9_]', '_', branch_identifier).upper()
                            # Remove multiple underscores
                            branch_identifier = re.sub(r'_+', '_', branch_identifier).strip('_')
                            logger.info(f"Extracted branch identifier: {branch_identifier}")
                            return branch_identifier
                        except StopIteration:
                            # BNCTL not found, take RE[digits] + up to 3 words after
                            branch_parts = parts[0:4]
                            branch_identifier = '_'.join(branch_parts)
                            branch_identifier = re.sub(r'[^A-Za-z0-9_]', '_', branch_identifier).upper()
                            branch_identifier = re.sub(r'_+', '_', branch_identifier).strip('_')
                            logger.info(f"Extracted branch identifier (fallback): {branch_identifier}")
                            return branch_identifier
                
                # Stop after checking reasonable number of lines (first 30 lines)
                if line_num > 30:
                    break
            
            logger.warning("Could not find RE[digits] pattern in first 30 lines of file")
            return 'UNKNOWN_BRANCH'
            
    except Exception as e:
        logger.error(f"Error reading file for branch extraction: {e}")
        return 'UNKNOWN_BRANCH'


def create_minio_client(endpoint: str, access_key: str, secret_key: str, secure: bool = True) -> Minio:
    """
    Create MinIO client connection
    """
    try:
        # Clean endpoint - remove http:// or https:// prefix and any path
        clean_endpoint = endpoint
        if clean_endpoint.startswith('http://'):
            clean_endpoint = clean_endpoint[7:]  # Remove 'http://'
            if secure:
                logger.warning("HTTP endpoint provided but secure=True, switching to secure=False")
                secure = False
        elif clean_endpoint.startswith('https://'):
            clean_endpoint = clean_endpoint[8:]  # Remove 'https://'
            secure = True
        
        # Remove any path component (everything after the first /)
        if '/' in clean_endpoint:
            clean_endpoint = clean_endpoint.split('/')[0]
        
        logger.info(f"Connecting to MinIO at {clean_endpoint} (secure={secure})")
        
        client = Minio(
            endpoint=clean_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        # Test connection
        client.list_buckets()
        logger.info(f"Successfully connected to MinIO at {clean_endpoint}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        raise


def list_s3_files(client: Minio, bucket_name: str, prefix: str = '', file_pattern: str = '*') -> List[str]:
    """
    List all files in S3 bucket with optional prefix and pattern matching
    """
    files_to_process = []
    
    try:
        logger.info(f"Scanning S3 bucket: {bucket_name}")
        logger.info(f"Prefix: {prefix or 'None'}")
        logger.info(f"File pattern: {file_pattern}")
        
        # List all objects in bucket with prefix
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        for obj in objects:
            # Skip directories
            if obj.object_name.endswith('/'):
                continue
                
            # Extract filename for pattern matching
            filename = obj.object_name.split('/')[-1]
            
            # Simple pattern matching
            if file_pattern == '*':
                files_to_process.append(obj.object_name)
            elif '*' in file_pattern:
                # Simple wildcard matching
                pattern = file_pattern.replace('*', '')
                if pattern in filename:
                    files_to_process.append(obj.object_name)
            elif file_pattern in filename:
                files_to_process.append(obj.object_name)
        
        logger.info(f"Found {len(files_to_process)} files to process")
        
        # Log first few files found for preview
        if files_to_process:
            logger.info("Files to process:")
            for i, file_path in enumerate(files_to_process[:10]):
                filename = file_path.split('/')[-1]
                logger.info(f"  {i+1}. {filename}")
            if len(files_to_process) > 10:
                logger.info(f"  ... and {len(files_to_process) - 10} more files")
        
        return files_to_process
        
    except S3Error as e:
        logger.error(f"S3 error while listing files: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        return []


def download_and_process_s3_file(client: Minio, bucket_name: str, object_name: str, output_dir: Path, args) -> Dict[str, any]:
    """
    Download file from S3 and process it, return results
    """
    filename = object_name.split('/')[-1]
    result = {
        'file': filename,
        'success': False,
        'file_type': None,
        'branch_identifier': None,
        'output_files': [],
        'stats': {},
        'error': None
    }
    
    temp_file_path = None
    
    try:
        # Download file to temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=f'_{filename}') as temp_file:
            temp_file_path = temp_file.name
            
        logger.debug(f"Downloading {object_name} to {temp_file_path}")
        client.fget_object(bucket_name, object_name, temp_file_path)
        
        # Detect file type and create prefix
        file_type_prefix = detect_file_type(temp_file_path)
        result['file_type'] = file_type_prefix
        
        # Extract branch name
        branch_identifier = extract_branch_name(temp_file_path)
        result['branch_identifier'] = branch_identifier
        
        # Skip CRB files
        if file_type_prefix == 'CRB':
            logger.info(f"Skipping CRB file: {filename}")
            result['error'] = 'CRB files not supported'
            return result
        
        # Skip unknown/unsupported file types
        if file_type_prefix == 'UNKNOWN':
            logger.info(f"Skipping unknown file type: {filename}")
            result['error'] = 'Unknown file type'
            return result
        
        # Add prefix to output name
        if args.output_name == 'general_ledger_structured':  # Default name
            original_name = filename
            output_base = f"{file_type_prefix}_{branch_identifier}_{original_name}"
        else:
            output_base = f"{file_type_prefix}_{branch_identifier}_{args.output_name}"
        
        logger.info(f"Processing: {filename} -> {output_base}")
        
        # Create parser and parse file
        gl_parser = GLParser()
        df = gl_parser.parse_file(temp_file_path)
        
        # Validate balance equation
        balance_errors = gl_parser.validate_balance_equation(df)
        
        # Calculate totals
        totals = gl_parser.calculate_totals(df)
        
        # Export files
        output_files = []
        
        # Export to CSV
        csv_path = output_dir / f'{output_base}.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        output_files.append(csv_path)
        
        # Export to Excel
        excel_path = output_dir / f'{output_base}.xlsx'
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='General Ledger')
            worksheet = writer.sheets['General Ledger']
            # Set column widths
            worksheet.column_dimensions['A'].width = 8
            worksheet.column_dimensions['B'].width = 60
            worksheet.column_dimensions['C'].width = 20
            worksheet.column_dimensions['D'].width = 20
            worksheet.column_dimensions['E'].width = 20
            worksheet.column_dimensions['F'].width = 20
        output_files.append(excel_path)
        
        # Export to JSON
        json_path = output_dir / f'{output_base}.json'
        df.to_json(json_path, orient='records', indent=2)
        output_files.append(json_path)
        
        # Export statistics
        stats_path = output_dir / f'{output_base}_stats.txt'
        with open(stats_path, 'w') as f:
            f.write("GENERAL LEDGER PARSING STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"S3 Object: {object_name}\n")
            f.write(f"Bucket: {bucket_name}\n")
            f.write(f"Total Lines: {gl_parser.stats['total_lines']:,}\n")
            f.write(f"Data Rows: {gl_parser.stats['data_rows']:,}\n")
            f.write(f"Header Rows: {gl_parser.stats['header_rows']:,}\n")
            f.write(f"Account Codes: {gl_parser.stats['account_codes']:,}\n")
            f.write(f"Continuation Lines: {gl_parser.stats['continuation_lines']:,}\n")
            f.write(f"Skipped Rows: {gl_parser.stats['skipped_rows']:,}\n")
            f.write(f"Output Rows: {len(df):,}\n\n")
            f.write("TOTALS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Opening Balance:   ${totals['opening_balance']:,.2f}\n")
            f.write(f"Debit Movements:   ${totals['debit_movements']:,.2f}\n")
            f.write(f"Credit Movements:  ${totals['credit_movements']:,.2f}\n")
            f.write(f"Closing Balance:   ${totals['closing_balance']:,.2f}\n\n")
            if balance_errors:
                f.write(f"BALANCE VALIDATION ERRORS ({len(balance_errors)})\n")
                f.write("-" * 80 + "\n")
                for error in balance_errors:
                    f.write(f"{error}\n")
                f.write("\n")
            if gl_parser.stats['warnings']:
                f.write(f"WARNINGS ({len(gl_parser.stats['warnings'])})\n")
                f.write("-" * 80 + "\n")
                for warning in gl_parser.stats['warnings']:
                    f.write(f"{warning}\n")
        output_files.append(stats_path)
        
        result['success'] = True
        result['output_files'] = output_files
        result['stats'] = {
            'total_lines': gl_parser.stats['total_lines'],
            'data_rows': gl_parser.stats['data_rows'],
            'output_rows': len(df),
            'balance_errors': len(balance_errors),
            'warnings': len(gl_parser.stats['warnings']),
            'totals': totals
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}")
        result['error'] = str(e)
        return result
    
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.debug(f"Cleaned up temp file: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp file {temp_file_path}: {e}")


def detect_file_type(input_file_path: str) -> str:
    """
    Detect file type based on file header content
    Returns prefix for output files: CRF_GL, CRF_PL, CRC_GL, CRC_PL, CRB
    """
    file_path = Path(input_file_path)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read first 1500 chars to check header (increased for better detection)
            header_content = f.read(1500).upper()
            
            logger.debug(f"Header content preview: {header_content[:200]}...")
            
            # First check if this is a valid banking file format
            # Must have RE000010 and BNCTL patterns
            if 'RE000010' not in header_content or 'BNCTL' not in header_content:
                logger.warning("File does not contain required banking format patterns (RE000010, BNCTL)")
                return 'UNKNOWN'
            
            # Detect CRB first (special case)
            if 'BALANCE DETAILS' in header_content and 'IN USD' in header_content:
                logger.info("Detected file type: CRB (Balance Details)")
                return 'CRB'
            
            # Must have either GENERAL LEDGER or PROFIT AND LOSS
            if 'PROFIT AND LOSS' in header_content:
                ledger_type = 'PL'
                logger.debug("Detected ledger type: PL (Profit and Loss)")
            elif 'GENERAL LEDGER' in header_content:
                ledger_type = 'GL'
                logger.debug("Detected ledger type: GL (General Ledger)")
            else:
                logger.warning("File does not contain GENERAL LEDGER or PROFIT AND LOSS patterns")
                return 'UNKNOWN'
            
            # Detect CRC vs CRF based on LIVE SYSTEM
            if 'LIVE SYSTEM' in header_content:
                # This is CRC file
                file_prefix = 'CRC'
                logger.debug("Detected file prefix: CRC (Live System)")
            else:
                # This is CRF file (all branches that don't have LIVE SYSTEM)
                # But must still be a valid banking file
                file_prefix = 'CRF'
                logger.debug("Detected file prefix: CRF (Branch System)")
                
            final_type = f"{file_prefix}_{ledger_type}"
            logger.info(f"Detected file type: {final_type}")
            return final_type
            
    except Exception as e:
        logger.error(f"Error reading file for type detection: {e}")
        # Fallback to path-based detection
        logger.warning("Falling back to path-based detection...")
        path_str = str(file_path).upper()
        
        if 'CRB' in path_str:
            logger.info("Detected file type from path: CRB")
            return 'CRB'
        elif 'CRF' in path_str and 'PL' in path_str:
            logger.info("Detected file type from path: CRF_PL") 
            return 'CRF_PL'
        elif 'CRF' in path_str:
            logger.info("Detected file type from path: CRF_GL")
            return 'CRF_GL'
        elif 'CRC' in path_str and 'PL' in path_str:
            logger.info("Detected file type from path: CRC_PL")
            return 'CRC_PL'
        elif 'CRC' in path_str:
            logger.info("Detected file type from path: CRC_GL")
            return 'CRC_GL'
        else:
            logger.error("Could not detect file type from header or path")
            return 'UNKNOWN'


def find_latest_summary_files(output_dir: Path) -> Dict[str, List[str]]:
    """
    Two-phase file selection logic:
    1. First phase: Find latest GL (>654 rows) and PL (>242 rows) files for each branch
    2. Second phase: For branches with no detailed files, take latest file regardless of row count
    Returns dict with 'csv_files' and 'txt_files' lists.
    """
    # Find all CSV files
    csv_files = list(output_dir.glob("CRF_*.csv"))
    
    # Store all files by branch and type for both phases
    all_files = {'GL': {}, 'PL': {}}  # All files for fallback
    detailed_files = {'GL': {}, 'PL': {}}  # Files meeting row criteria
    
    logger.info("Finding latest summary files with two-phase selection...")
    
    # Parse all files and categorize them
    for csv_file in csv_files:
        # Parse filename: CRF_GL_RE000010_BRANCH_NAME_20250122_20260121_141841_21206894945151800.csv
        # Or fallback to old format: CRF_GL_RE000010_BRANCH_NAME_21206894945151800.csv
        match_new = re.match(r'CRF_(GL|PL)_(RE\d+_.*?)_(\d{8})_(\d{8})_(\d{6})_(\d+)\.csv', csv_file.name)
        match_old = re.match(r'CRF_(GL|PL)_(RE\d+_.*?)_(\d+)\.csv', csv_file.name)
        
        if match_new:
            file_type = match_new.group(1)
            branch = match_new.group(2)
            cob_date = match_new.group(3)  # 20250122 (COB date)
            create_date = match_new.group(4)  # 20260121 (create date)
            time_part = match_new.group(5)  # 141841
            t24_timestamp = match_new.group(6)
            datetime_key = f"{create_date}_{time_part}"  # Use create date for latest logic
        elif match_old:
            file_type = match_old.group(1)
            branch = match_old.group(2)
            t24_timestamp = match_old.group(3)
            datetime_key = t24_timestamp  # Fallback to T24 timestamp comparison
        else:
            continue
        
        # Count rows
        try:
            df = pd.read_csv(csv_file)
            row_count = len(df)
        except:
            continue
        
        file_info = {
            'file': csv_file.name,
            'datetime_key': datetime_key,
            'path': csv_file,
            'rows': row_count
        }
        
        # Store in all_files for potential fallback (Phase 2)
        if branch not in all_files[file_type]:
            all_files[file_type][branch] = []
        all_files[file_type][branch].append(file_info)
        
        # Phase 1: Check criteria for detailed files (GL > 654, PL > 242)
        target_rows_gl = 654
        target_rows_pl = 242
        
        if (file_type == 'GL' and row_count > target_rows_gl) or (file_type == 'PL' and row_count > target_rows_pl):
            # For each branch + file_type combination, keep ONLY the latest detailed file
            if branch not in detailed_files[file_type]:
                # First detailed file for this branch + file_type
                detailed_files[file_type][branch] = file_info
                logger.debug(f"Phase 1: Found first detailed {file_type} for {branch}: {file_info['file']} ({row_count} rows)")
            elif datetime_key > detailed_files[file_type][branch]['datetime_key']:
                # Replace with newer detailed file
                old_file = detailed_files[file_type][branch]['file']
                detailed_files[file_type][branch] = file_info
                logger.debug(f"Phase 1: Replaced {file_type} for {branch}: {old_file} -> {file_info['file']} ({row_count} rows)")
    
    # Phase 2: For branches without detailed files, find latest file regardless of row count
    logger.info("\n--- Phase 2: Fallback for branches without detailed files ---")
    for file_type in ['GL', 'PL']:
        for branch, files_list in all_files[file_type].items():
            if branch not in detailed_files[file_type]:
                # No detailed files found for this branch + file_type combination
                # Take latest file regardless of row count (could be summary file with exactly 654/242 rows)
                latest_file = max(files_list, key=lambda x: x['datetime_key'])
                detailed_files[file_type][branch] = latest_file
                logger.info(f"Phase 2 Fallback: Using latest {file_type} for {branch}: {latest_file['file']} ({latest_file['rows']} rows)")
    
    # Log Phase 1 results
    logger.info(f"\n--- Phase 1 Results: Detailed Files (GL > 654, PL > 242) ---")
    phase1_count = 0
    for file_type in ['GL', 'PL']:
        target_rows = 654 if file_type == 'GL' else 242
        for branch, info in detailed_files[file_type].items():
            if info['rows'] > target_rows:
                logger.info(f"Phase 1 Selected: {file_type} for {branch}: {info['file']} ({info['rows']} rows)")
                phase1_count += 1
    
    if phase1_count == 0:
        logger.info("No detailed files found in Phase 1, all selections from Phase 2 fallback")
    
    # Collect final results
    latest_files = {'csv_files': [], 'txt_files': [], 'date_folder': None}
    
    logger.info(f"\n--- Final Selection Results ---")
    total_files_selected = 0
    
    for file_type in ['GL', 'PL']:
        target_rows = 654 if file_type == 'GL' else 242
        logger.info(f"\n{file_type} Files Selected:")
        
        for branch, info in detailed_files[file_type].items():
            csv_path = info['path']
            base_name = csv_path.stem
            
            # Add CSV file
            latest_files['csv_files'].append(str(csv_path))
            
            # Add corresponding TXT stats file
            txt_path = csv_path.parent / f"{base_name}_stats.txt"
            if txt_path.exists():
                latest_files['txt_files'].append(str(txt_path))
            
            # Extract date for folder structure (from first file)
            if not latest_files['date_folder']:
                # Try to extract COB date from new filename format first
                match_date = re.match(r'CRF_(GL|PL)_(RE\d+_.*?)_(\d{8})_(\d{8})_(\d{6})_(\d+)', csv_path.name)
                if match_date:
                    cob_date_str = match_date.group(3)  # 20250122 (COB date)
                    latest_files['date_folder'] = f"{cob_date_str[0:4]}-{cob_date_str[4:6]}-{cob_date_str[6:8]}"  # 2025-01-22
                else:
                    # Fallback: use today's date
                    from datetime import datetime
                    latest_files['date_folder'] = datetime.now().strftime('%Y-%m-%d')
            
            # Log selection with clear indication of selection reason
            if info['rows'] > target_rows:
                logger.info(f"  ✓ {branch}: {info['file']} ({info['rows']} rows) [DETAILED - Phase 1]")
            else:
                logger.info(f"  ✓ {branch}: {info['file']} ({info['rows']} rows) [FALLBACK - Phase 2]")
            
            total_files_selected += 1
    
    logger.info(f"\n--- SUMMARY ---")
    logger.info(f"Total files selected: {total_files_selected}")
    logger.info(f"CSV files: {len(latest_files['csv_files'])}")
    logger.info(f"TXT files: {len(latest_files['txt_files'])}")
    logger.info(f"Target date folder: {latest_files['date_folder']}")
    logger.info(f"Selection logic: Phase 1 (detailed) + Phase 2 (fallback)")
    
    return latest_files


def process_date_batch(minio_client: Minio, bucket: str, target_date: str, args, output_dir: Path) -> Dict[str, any]:
    """
    Process all files for a specific date
    Returns processing results for the date
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"PROCESSING DATE: {target_date}")
    logger.info(f"{'='*80}")
    
    # Build prefix for the target date
    prefix = f"{args.prefix}{target_date}/" if args.prefix else f"accounting_full/{target_date}/"
    
    logger.info(f"Looking for files with prefix: {prefix}")
    
    # List files for this date
    files_to_process = list_s3_files(
        client=minio_client,
        bucket_name=bucket,
        prefix=prefix,
        file_pattern=args.file_pattern
    )
    
    if not files_to_process:
        logger.warning(f"No files found for date {target_date} with prefix {prefix}")
        return {
            'date': target_date,
            'success': False,
            'files_processed': 0,
            'files_found': 0,
            'error': 'No files found for this date'
        }
    
    logger.info(f"Found {len(files_to_process)} files for {target_date}")
    
    # Process files for this date
    results = []
    successful_files = 0
    failed_files = 0
    skipped_files = 0
    
    for i, object_name in enumerate(files_to_process, 1):
        filename = object_name.split('/')[-1]
        logger.info(f"[{i}/{len(files_to_process)}] Processing: {filename}")
        
        result = download_and_process_s3_file(minio_client, bucket, object_name, output_dir, args)
        results.append(result)
        
        if result['success']:
            successful_files += 1
            logger.info(f"✓ Successfully processed: {filename}")
        elif result['error'] in ['CRB files not supported', 'Unknown file type']:
            skipped_files += 1
            logger.info(f"⚠ Skipped: {filename} ({result['error']})")
        else:
            failed_files += 1
            logger.error(f"✗ Failed: {filename} - {result['error']}")
    
    # Find and upload latest summary files for this date
    bronze_success = False
    if successful_files > 0 and args.upload_bronze:
        logger.info(f"\nFinding latest summary files for {target_date}...")
        
        try:
            latest_files = find_latest_summary_files(output_dir)
            
            if latest_files['csv_files']:
                # Override the date folder with target date
                latest_files['date_folder'] = target_date
                bronze_success = upload_files_to_bronze(minio_client, latest_files)
                
                if bronze_success:
                    logger.info(f"✓ Bronze bucket upload completed for {target_date}")
                else:
                    logger.error(f"✗ Bronze bucket upload failed for {target_date}")
            else:
                logger.warning(f"No latest summary files found for {target_date}")
                
        except Exception as e:
            logger.error(f"Error during bronze bucket upload for {target_date}: {e}")
    
    return {
        'date': target_date,
        'success': successful_files > 0 and bronze_success if args.upload_bronze else successful_files > 0,
        'files_processed': successful_files,
        'files_found': len(files_to_process),
        'files_failed': failed_files,
        'files_skipped': skipped_files,
        'bronze_upload': bronze_success if args.upload_bronze else 'Not requested',
        'results': results
    }


def upload_files_to_bronze(minio_client: Minio, latest_files: Dict[str, List[str]]) -> bool:
    """
    Upload latest summary files to bronze bucket with mixed structure:
    - CSV files go to: bronze/accounting_parsed/data/{date}/{branch_name}/{file_type}/*.csv
    - TXT files go to: bronze/accounting_parsed/summary/{date}/*.txt
    """
    bronze_bucket = "bronze"
    date_folder = latest_files['date_folder']
    
    try:
        # Process CSV files
        logger.info(f"Uploading {len(latest_files['csv_files'])} CSV files to bronze bucket with new structure...")
        
        for csv_file in latest_files['csv_files']:
            file_path = Path(csv_file)
            filename = file_path.name
            
            # Extract branch name and file type from filename
            # Format: CRF_GL_RE000010_BRANCH_NAME_20250122_20260121_141841_21206894945151800.csv
            match = re.match(r'(CRF|CRC)_(GL|PL)_(RE\d+_.*?)_(\d{8})_(\d{8})_(\d{6})_(\d+)\.csv', filename)
            if match:
                file_prefix = match.group(1)  # CRF or CRC
                ledger_type = match.group(2)  # GL or PL
                branch_name = match.group(3)  # RE000010_BRANCH_NAME
                file_type = f"{file_prefix}_{ledger_type}"  # CRF_GL, CRF_PL, etc.
                
                # Create object path: bronze/accounting_parsed/data/{date}/{branch_name}/{file_type}/filename.csv
                object_name = f"accounting_parsed/data/{date_folder}/{branch_name}/{file_type}/{filename}"
                
                minio_client.fput_object(bronze_bucket, object_name, csv_file)
                logger.info(f"✓ Uploaded CSV: s3://{bronze_bucket}/{object_name}")
            else:
                # Fallback for files that don't match expected format
                object_name = f"accounting_parsed/data/{date_folder}/UNKNOWN_BRANCH/UNKNOWN_TYPE/{filename}"
                minio_client.fput_object(bronze_bucket, object_name, csv_file)
                logger.warning(f"⚠ Uploaded CSV (unknown format): s3://{bronze_bucket}/{object_name}")
        
        # Process TXT files - keep in summary folder (same level as data)
        txt_prefix = f"accounting_parsed/summary/{date_folder}/"
        logger.info(f"Uploading {len(latest_files['txt_files'])} TXT files to bronze bucket: {txt_prefix}")
        
        for txt_file in latest_files['txt_files']:
            file_path = Path(txt_file)
            filename = file_path.name
            
            # TXT files go to summary folder at same level as data folder
            object_name = f"{txt_prefix}{filename}"
            
            minio_client.fput_object(bronze_bucket, object_name, txt_file)
            logger.info(f"✓ Uploaded TXT: s3://{bronze_bucket}/{object_name}")
        
        logger.info("=" * 80)
        logger.info("BRONZE BUCKET UPLOAD COMPLETED")
        logger.info("=" * 80)
        logger.info(f"CSV structure: s3://{bronze_bucket}/accounting_parsed/data/{date_folder}/{{branch_name}}/{{file_type}}/")
        logger.info(f"TXT structure: s3://{bronze_bucket}/accounting_parsed/summary/{date_folder}/")
        logger.info(f"Example CSV: s3://{bronze_bucket}/accounting_parsed/data/{date_folder}/RE000010_SUKURSAL_DILI/CRF_GL/")
        logger.info(f"Example TXT: s3://{bronze_bucket}/accounting_parsed/summary/{date_folder}/")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload files to bronze bucket: {e}")
        return False


def main():
    """Main execution function"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Parse General Ledger text files into structured data (CSV, Excel, JSON)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - process specific date:
  export MINIO_ENDPOINT=10.0.40.115:31002
  export MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE  
  export MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  export MINIO_BUCKET=raw
  python gl_parser.py --date 2025-01-22 --upload-bronze -o ./output/
  
  # Process specific prefix/path:
  python gl_parser.py --prefix accounting_full/2026-01-21/ -o ./output/
  
  # Process with specific date and upload to bronze:
  python gl_parser.py --date 2025-01-22 --upload-bronze
  
  # Command line arguments (override env vars):
  python gl_parser.py --endpoint 10.0.40.115:31002 --bucket raw --access-key KEY --secret-key SECRET --no-ssl --date 2025-01-22
        """
    )
    
    parser.add_argument(
        '--endpoint',
        help='MinIO S3 endpoint (e.g., minio.example.com or localhost:9000). Can be set via MINIO_ENDPOINT env var'
    )
    
    parser.add_argument(
        '--bucket',
        help='S3 bucket name to process files from. Can be set via MINIO_BUCKET env var'
    )
    
    parser.add_argument(
        '--access-key',
        help='S3 access key. Can be set via MINIO_ACCESS_KEY env var'
    )
    
    parser.add_argument(
        '--secret-key',
        help='S3 secret key. Can be set via MINIO_SECRET_KEY env var'
    )
    
    parser.add_argument(
        '--prefix',
        default='',
        help='S3 object prefix to filter files (e.g., "data/2024/", "accounting_full/")'
    )
    
    parser.add_argument(
        '--no-ssl',
        action='store_true',
        help='Use HTTP instead of HTTPS (for local MinIO)'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        help='Output directory for generated files (default: ./output/)',
        default='./output/'
    )
    
    parser.add_argument(
        '-n', '--output-name',
        help='Base name for output files (default: general_ledger_structured)',
        default='general_ledger_structured'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        help='Enable verbose logging (debug mode)',
        action='store_true'
    )
    
    parser.add_argument(
        '--file-pattern',
        help='File pattern to match (e.g., "*.txt", "*crf*"). Default: process all files',
        default='*'
    )
    
    parser.add_argument(
        '--upload-bronze',
        action='store_true',
        help='Upload latest summary files to bronze layer after processing'
    )
    
    parser.add_argument(
        '--date',
        help='Specific date to process in YYYY-MM-DD format (e.g., 2025-01-22)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get configuration from environment variables or command line arguments
    endpoint = args.endpoint or os.getenv('MINIO_ENDPOINT')
    bucket = args.bucket or os.getenv('MINIO_BUCKET')
    access_key = args.access_key or os.getenv('MINIO_ACCESS_KEY')
    secret_key = args.secret_key or os.getenv('MINIO_SECRET_KEY')
    prefix = args.prefix or os.getenv('MINIO_PREFIX')
    
    # Validate required parameters
    missing_params = []
    if not endpoint:
        missing_params.append('endpoint (--endpoint or MINIO_ENDPOINT)')
    if not bucket:
        missing_params.append('bucket (--bucket or MINIO_BUCKET)')
    if not access_key:
        missing_params.append('access_key (--access-key or MINIO_ACCESS_KEY)')
    if not secret_key:
        missing_params.append('secret_key (--secret-key or MINIO_SECRET_KEY)')
    
    if missing_params:
        logger.error("Missing required parameters:")
        for param in missing_params:
            logger.error(f"  - {param}")
        logger.error("\\nSet them as environment variables or command line arguments.")
        logger.error("Example:")
        logger.error("  export MINIO_ENDPOINT=10.0.40.115:31002")
        logger.error("  export MINIO_BUCKET=raw")
        logger.error("  export MINIO_ACCESS_KEY=your_access_key")
        logger.error("  export MINIO_SECRET_KEY=your_secret_key")
        sys.exit(1)
    
    # Create MinIO client
    try:
        minio_client = create_minio_client(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=not args.no_ssl
        )
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        sys.exit(1)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine processing mode
    if args.date:
        # Process specific date
        target_date = args.date
        logger.info("=" * 80)
        logger.info("S3 BATCH GENERAL LEDGER PARSER - DATE MODE")
        logger.info("=" * 80)
        logger.info(f"S3 Endpoint:       {endpoint}")
        logger.info(f"S3 Bucket:         {bucket}")
        logger.info(f"Target Date:       {target_date}")
        logger.info(f"Output dir:        {output_dir}")
        logger.info(f"Base Prefix:       {prefix or 'accounting_full/'}")
        logger.info(f"Upload Bronze:     {args.upload_bronze}")
        logger.info("=" * 80)
        
        # Process the specific date
        date_result = process_date_batch(minio_client, bucket, target_date, args, output_dir)
        
        if date_result['success']:
            logger.info(f"✅ Date {target_date} processed successfully")
            logger.info(f"Files processed: {date_result['files_processed']}")
            logger.info(f"Bronze upload: {date_result.get('bronze_upload', 'N/A')}")
        else:
            logger.error(f"❌ Date {target_date} processing failed: {date_result.get('error', 'Unknown error')}")
            sys.exit(1)
    
    else:
        # Regular mode - process files with prefix
        # List files to process from S3
        files_to_process = list_s3_files(
            client=minio_client,
            bucket_name=bucket,
            prefix=prefix,
            file_pattern=args.file_pattern
        )
        
        if not files_to_process:
            logger.error("No files found to process in S3 bucket")
            sys.exit(1)
        
        # Process files
        logger.info("=" * 80)
        logger.info("S3 BATCH GENERAL LEDGER PARSER - REGULAR MODE")
        logger.info("=" * 80)
        logger.info(f"S3 Endpoint:       {endpoint}")
        logger.info(f"S3 Bucket:         {bucket}")
        logger.info(f"S3 Prefix:         {prefix or 'None'}")
        logger.info(f"Files to process:  {len(files_to_process)}")
        logger.info(f"Output dir:        {output_dir}")
        logger.info(f"File pattern:      {args.file_pattern}")
        logger.info(f"SSL:               {not args.no_ssl}")
        logger.info("=" * 80)
        
        # Process each file
        results = []
        successful_files = 0
        failed_files = 0
        skipped_files = 0
        
        for i, object_name in enumerate(files_to_process, 1):
            filename = object_name.split('/')[-1]
            logger.info(f"\n[{i}/{len(files_to_process)}] Processing: {filename}")
            
            result = download_and_process_s3_file(minio_client, bucket, object_name, output_dir, args)
            results.append(result)
            
            if result['success']:
                successful_files += 1
                logger.info(f"✓ Successfully processed: {filename}")
                logger.info(f"  Generated {len(result['output_files'])} output files")
            elif result['error'] in ['CRB files not supported', 'Unknown file type']:
                skipped_files += 1
                logger.info(f"⚠ Skipped: {filename} ({result['error']})")
            else:
                failed_files += 1
                logger.error(f"✗ Failed: {filename} - {result['error']}")
        
        # Print final summary
        logger.info(f"\n{'=' * 80}")
        logger.info("S3 BATCH PROCESSING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"S3 Bucket:             {bucket}")
        logger.info(f"Total files found:     {len(files_to_process)}")
        logger.info(f"Successfully processed: {successful_files}")
        logger.info(f"Skipped files:         {skipped_files}")
        logger.info(f"Failed files:          {failed_files}")
        logger.info(f"Output directory:      {output_dir}")
        
        # Show breakdown by file type
        type_summary = {}
        for result in results:
            if result['success']:
                file_type = result['file_type']
                if file_type not in type_summary:
                    type_summary[file_type] = 0
                type_summary[file_type] += 1
        
        if type_summary:
            logger.info("\nFiles processed by type:")
            for file_type, count in type_summary.items():
                logger.info(f"  {file_type}: {count} files")
        
        # Show failed files details
        failed_results = [r for r in results if not r['success'] and r['error'] not in ['CRB files not supported', 'Unknown file type']]
        if failed_results:
            logger.info("\nFailed files:")
            for result in failed_results:
                logger.info(f"  {result['file']}: {result['error']}")
        
        # Export batch summary
        summary_path = output_dir / 'batch_processing_summary.txt'
        with open(summary_path, 'w') as f:
            f.write("S3 BATCH PROCESSING SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"S3 Endpoint: {endpoint}\n")
            f.write(f"S3 Bucket: {bucket}\n")
            f.write(f"S3 Prefix: {prefix or 'None'}\n")
            f.write(f"Output Directory: {output_dir}\n")
            f.write(f"Processing Time: {pd.Timestamp.now()}\n\n")
            f.write(f"Total Files Found: {len(files_to_process)}\n")
            f.write(f"Successfully Processed: {successful_files}\n")
            f.write(f"Skipped Files: {skipped_files}\n")
            f.write(f"Failed Files: {failed_files}\n\n")
            
            f.write("DETAILED RESULTS\n")
            f.write("-" * 80 + "\n")
            for result in results:
                f.write(f"File: {result['file']}\n")
                f.write(f"  Success: {result['success']}\n")
                f.write(f"  Type: {result['file_type']}\n")
                f.write(f"  Branch: {result['branch_identifier']}\n")
                if result['success']:
                    stats = result['stats']
                    f.write(f"  Data Rows: {stats['data_rows']:,}\n")
                    f.write(f"  Output Rows: {stats['output_rows']:,}\n")
                    f.write(f"  Output Files: {len(result['output_files'])}\n")
                elif result['error']:
                    f.write(f"  Error: {result['error']}\n")
                f.write("\n")
        
        logger.info(f"\n✓ S3 batch summary saved: {summary_path}")
        
        # Upload latest summary files to bronze layer
        if args.upload_bronze:
            logger.info("\n" + "=" * 80)
            logger.info("UPLOADING LATEST SUMMARY FILES TO BRONZE BUCKET")
            logger.info("=" * 80)
            
            try:
                latest_files = find_latest_summary_files(output_dir)
                
                if latest_files['csv_files']:
                    success = upload_files_to_bronze(minio_client, latest_files)
                    if success:
                        logger.info("✓ Bronze bucket upload completed successfully")
                    else:
                        logger.error("✗ Bronze bucket upload failed")
                else:
                    logger.warning("No latest summary files found to upload")
                    
            except Exception as e:
                logger.info(f"Error during bronze bucket upload: {e}")
        else:
            logger.info("\nSkipping bronze bucket upload (use --upload-bronze to enable)")
        
        if failed_files > 0:
            sys.exit(1)  # Exit with error code if any files failed
    
    logger.info("=" * 80)


if __name__ == "__main__":
    main()