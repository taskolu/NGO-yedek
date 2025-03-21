import pandas as pd
import logging
from pathlib import Path
from typing import List
from .pdf_processor import MTCNEntry  # Reuse the same data structure

class XLSXProcessor:
    def __init__(self, logging_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)

    def _find_network_total(self, df: pd.DataFrame) -> float:
        """Find the network total value that appears under 'Total'"""
        try:
            # Look for 'Total' cell
            for idx, row in df.iterrows():
                for col in df.columns:
                    cell_value = str(row[col]).strip().upper()
                    if cell_value == "TOTAL":
                        # Check the cell below for the amount
                        if idx + 1 < len(df):
                            total_value = str(df.iloc[idx + 1][col]).strip()
                            if total_value and total_value.lower() != 'nan':
                                try:
                                    # Convert to float, handling currency symbols and commas
                                    return float(total_value.replace('$', '').replace(',', ''))
                                except ValueError:
                                    self.logger.warning(f"Failed to parse network total value: {total_value}")
            
            self.logger.warning("Could not find network total value under 'Total' cell")
            return 0.0
        except Exception as e:
            self.logger.error(f"Error finding network total: {str(e)}")
            return 0.0

    def extract_from_file(self, xlsx_path: Path) -> List[MTCNEntry]:
        """Extract MTCN numbers and amounts from an XLSX file."""
        self.logger.info(f"Processing XLSX: {xlsx_path}")
        
        try:
            # Read the first sheet
            df = pd.read_excel(xlsx_path, sheet_name=0)
            
            # Find the network total first
            network_total = self._find_network_total(df)
            
            # Find the row containing column headers
            header_row = None
            mtcn_col = None
            amount_col = None
            
            # First look for the header row containing "MTCN"
            for idx, row in df.iterrows():
                for col in df.columns:
                    cell_value = str(row[col]).strip().upper()
                    if "MTCN" in cell_value:
                        header_row = idx
                        break
                if header_row is not None:
                    break
            
            if header_row is None:
                raise ValueError("Could not find row containing MTCN header")
                
            # Get the data starting from the row after the header
            data_df = df.iloc[header_row + 1:].reset_index(drop=True)
            
            # Find MTCN and Amount columns from the header row
            for col in df.columns:
                header_value = str(df.iloc[header_row][col]).strip().upper()
                if "MTCN" in header_value:
                    mtcn_col = col
                elif "AMOUNT" in header_value:
                    amount_col = col
            
            if mtcn_col is None or amount_col is None:
                raise ValueError("Could not find MTCN or Amount columns in the Excel file")
            
            # Extract and clean data
            entries = []
            for _, row in data_df.iterrows():
                mtcn = str(row[mtcn_col]).strip()
                amount_str = str(row[amount_col]).strip()
                
                # Skip empty rows or non-numeric MTCNs
                if not mtcn or not amount_str or mtcn.lower() == 'nan':
                    continue
                    
                try:
                    # Convert amount to float, handling potential currency symbols
                    amount = float(amount_str.replace('$', '').replace(',', ''))
                    
                    # Validate MTCN format (10 digits)
                    mtcn = mtcn.replace('.0', '')  # Remove decimal if present
                    if len(mtcn) == 10 and mtcn.isdigit():
                        # Create entry with network_total
                        entry = MTCNEntry(mtcn=mtcn, amount=amount, page=1, line=0, network_total=network_total)
                        entries.append(entry)
                        self.logger.debug(f"Found entry: MTCN={mtcn}, Amount={amount}")
                except ValueError:
                    self.logger.warning(f"Failed to parse amount for MTCN {mtcn}: {amount_str}")
                    continue
            
            self.logger.info(f"Extracted {len(entries)} valid entries from XLSX")
            if network_total > 0:
                self.logger.info(f"Found network total: ${network_total:,.2f}")
            return entries
            
        except Exception as e:
            self.logger.error(f"Error processing {xlsx_path}: {str(e)}")
            raise
