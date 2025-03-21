from multiprocessing import get_context
import PyPDF2
import re
import logging
from pathlib import Path
from typing import List, Tuple, Dict
from dataclasses import dataclass

@dataclass
class MTCNEntry:
    mtcn: str
    amount: float
    page: int
    line: int
    network_total: float = 0.0

class PDFProcessor:
    def __init__(self, logging_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
       
        # Common pattern parts
        amount_pattern = r'-?\d{1,3}(?:,\d{3})*\.\d{2}'
        fx_comm_pattern = r'(?:-?\d{1,3}(?:,\d{3})*\.\d{2})'  # Made negative sign optional
        cnl_pattern = r'(?:\s+CNL)?'  # Optional CNL between MTCN and code
       
        # Patterns for different formats
        self.patterns = {
            'AUK': re.compile(
                rf'(\d{{10}}){cnl_pattern}\s+AUK\d+\s+({amount_pattern})\s+\d+\.\d{{2}}\s+\d+\.\d{{2}}\s+{fx_comm_pattern}(?:\s+{fx_comm_pattern})?\s+({amount_pattern})'
            ),
            'AUS': re.compile(
                rf'(\d{{10}}){cnl_pattern}\s+AUS\d+\s+({amount_pattern})\s+\d+\.\d{{2}}\s+\d+\.\d{{2}}\s+{fx_comm_pattern}(?:\s+{fx_comm_pattern})?\s+({amount_pattern})'
            ),
            'ATL': re.compile(
                rf'(\d{{10}}){cnl_pattern}\s+ATL\d+\s+({amount_pattern})\s+\d+\.\d{{2}}\s+\d+\.\d{{2}}\s+{fx_comm_pattern}(?:\s+{fx_comm_pattern})?\s+({amount_pattern})'
            )
        }

        # Additional pattern specifically for paid out transactions
        self.paid_pattern = re.compile(
            rf'(\d{{10}}){cnl_pattern}\s+(?:AUS|AUK|ATL)\d+\s+(-\d{{1,3}}(?:,\d{{3}})*\.\d{{2}})\s+\d+\.\d{{2}}\s+\d+\.\d{{2}}\s+{fx_comm_pattern}(?:\s+{fx_comm_pattern})?\s+({amount_pattern})'
        )

    def _process_page_text(self, text: str, page_num: int) -> List[MTCNEntry]:
        """Process text from a single PDF page."""
        entries = {}  # Using dict to prevent duplicates
        type_entries = {  # Separate dict for each type
            'AUS': {},
            'AUK': {},
            'ATL': {}
        }
        type_duplicates = {  # Track duplicates for each type
            'AUS': set(),
            'AUK': set(),
            'ATL': set()
        }
       
        # Debug print the raw text
        self.logger.debug(f"\n=== RAW TEXT FROM PAGE {page_num} ===\n{text}\n=== END PAGE ===\n")
       
        # Process each pattern
        for code, pattern in self.patterns.items():
            matches = pattern.finditer(text)
            for match in matches:
                try:
                    mtcn = match.group(1)
                    net_settlement = float(match.group(3).replace(',', ''))
                   
                    # Handle entries with duplicate checking for all types
                    if mtcn in type_entries[code]:
                        type_duplicates[code].add(mtcn)
                        self.logger.debug(f"Found duplicate {code} MTCN: {mtcn}, marking for removal")
                    else:
                        type_entries[code][mtcn] = MTCNEntry(mtcn, net_settlement, page_num, 0)
                        self.logger.debug(f"Found {code} entry: MTCN={mtcn}, Net Settlement={net_settlement}")
                   
                except (IndexError, ValueError) as e:
                    self.logger.warning(f"Failed to parse {code} match on page {page_num}: {match.group(0)}")
                    continue

        # Remove duplicates for all types
        for code in ['AUS', 'AUK', 'ATL']:
            for mtcn in type_duplicates[code]:
                if mtcn in type_entries[code]:
                    del type_entries[code][mtcn]
                    self.logger.info(f"Removed duplicate {code} MTCN: {mtcn} as it appears multiple times")

        # Combine all valid entries
        for code in ['AUS', 'AUK', 'ATL']:
            entries.update({k: v for k, v in type_entries[code].items() if k not in type_duplicates[code]})

        self.logger.info(f"Found {len(entries)} valid entries on page {page_num}")
        return list(entries.values())

    def extract_from_file(self, pdf_path: Path) -> List[MTCNEntry]:
        """Extract MTCN numbers and amounts from a single PDF file."""
        self.logger.info(f"Processing PDF: {pdf_path}")
       
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
               
                # First pass: find all paid out MTCNs (these should be excluded)
                paid_out_mtcns = set()
                network_total = 0.0
                is_paid_out_section = False
               
                # Process all pages to find network total and paid out MTCNs
                for page in reader.pages:
                    text = page.extract_text()
                   
                    # Look for network total
                    if "Network Total:" in text:
                        for line in text.split('\n'):
                            if "Network Total:" in line:
                                numbers = [float(n.replace(',', ''))
                                         for n in line.split()
                                         if n.replace(',', '').replace('.', '').replace('-', '').isdigit()]
                                if numbers:
                                    network_total = numbers[-1]
                                    break
               
                    # Check for paid out section header or continue if in paid out section
                    if "Transactions Paid Out to Consumers" in text:
                        is_paid_out_section = True
                   
                    if is_paid_out_section:
                        for match in self.paid_pattern.finditer(text):
                            try:
                                mtcn = match.group(1)
                                amount = float(match.group(2).replace(',', ''))
                                if amount < 0:
                                    paid_out_mtcns.add(mtcn)
                                    self.logger.debug(f"Found paid out MTCN: {mtcn} with amount {amount}")
                            except Exception as e:
                                self.logger.warning(f"Failed to parse paid out entry: {str(e)}")
                                continue
                       
                        # Check if we should continue to next page (look for any negative amounts)
                        has_negative = False
                        for line in text.split('\n'):
                            parts = line.split()
                            if len(parts) >= 2 and parts[-1].replace(',', '').replace('-', '').replace('.', '').isdigit():
                                try:
                                    if float(parts[-1].replace(',', '')) < 0:
                                        has_negative = True
                                        break
                                except ValueError:
                                    continue
                        if not has_negative:
                            is_paid_out_section = False

                # Second pass: extract valid entries
                valid_entries = {}
               
                for page_num, page in enumerate(reader.pages, 1):
                    text = page.extract_text()
                   
                    # Process entries only if they're not in paid_out_mtcns
                    for code, pattern in self.patterns.items():
                        matches = pattern.finditer(text)
                        for match in matches:
                            try:
                                mtcn = match.group(1)
                                # Skip if this MTCN was paid out
                                if mtcn in paid_out_mtcns:
                                    self.logger.debug(f"Skipping paid out MTCN: {mtcn}")
                                    continue
                                   
                                net_settlement = float(match.group(3).replace(',', ''))
                                # Skip if amount is 0.00
                                if net_settlement == 0.00:
                                    self.logger.debug(f"Skipping MTCN {mtcn} with 0.00 amount")
                                    continue
                                
                                if mtcn not in valid_entries:
                                    # Create entry with network total
                                    entry = MTCNEntry(mtcn, net_settlement, page_num, 0)
                                    entry.network_total = network_total
                                    valid_entries[mtcn] = entry
                                    self.logger.debug(f"Found {code} entry: MTCN={mtcn}, Net Settlement={net_settlement}")
                            except Exception as e:
                                self.logger.warning(f"Failed to parse {code} entry: {str(e)}")
                                continue
                   
                    self.logger.info(f"Processed page {page_num}")

                self.logger.info(f"Found {len(paid_out_mtcns)} paid out MTCNs")
                self.logger.info(f"Extracted {len(valid_entries)} valid entries")
                self.logger.info(f"Network Total: {network_total}")

                # Debug print extracted entries
                self.logger.info("Extracted MTCNs from PDF:")
                for entry in valid_entries.values():
                    self.logger.info(f"MTCN: {entry.mtcn} -> Amount: {entry.amount:.2f}")
               
                return list(valid_entries.values())

        except Exception as e:
            self.logger.error(f"Error processing {pdf_path}: {str(e)}")
            raise

    def process_directory(self, directory: Path) -> List[MTCNEntry]:
        """Process all PDF files in a directory."""
        all_entries = {}  # Using dict to prevent duplicates across files
        pdf_files = list(directory.glob('*.pdf'))
       
        self.logger.info(f"Found {len(pdf_files)} PDF files in {directory}")
       
        for pdf_file in pdf_files:
            try:
                entries = self.extract_from_file(pdf_file)
                # Add new entries to all_entries dict
                for entry in entries:
                    if entry.mtcn not in all_entries:
                        all_entries[entry.mtcn] = entry
                        self.logger.info(f"PDF Processing - MTCN: {entry.mtcn}, Amount: {entry.amount}")
            except Exception as e:
                self.logger.error(f"Failed to process {pdf_file}: {str(e)}")
                continue

        return list(all_entries.values())


