import subprocess
import os
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

class ConverterService:
    @staticmethod
    def _ensure_fixed_table_layout(docx_path: str):
        """
        Intervenes in the DOCX XML to set w:tblLayout type="fixed".
        This prevents table collapse during PDF conversion.
        """
        try:
            from docx import Document
            from docx.oxml.ns import qn
            from docx.oxml import OxmlElement
            
            doc = Document(docx_path)
            for table in doc.tables:
                tbl_pr = table._element.xpath('w:tblPr')
                if tbl_pr:
                    # Look for existing layout
                    layout = tbl_pr[0].xpath('w:tblLayout')
                    if not layout:
                        new_layout = OxmlElement('w:tblLayout')
                        new_layout.set(qn('w:type'), 'fixed')
                        tbl_pr[0].append(new_layout)
                    else:
                        layout[0].set(qn('w:type'), 'fixed')
            doc.save(docx_path)
            logger.info(f"Fixed table layout enforced for {docx_path}")
        except Exception as e:
            logger.warning(f"Failed to enforce fixed layout: {e}")

    @classmethod
    def convert_docx_to_pdf(cls, input_path: str, method: str = 'libreoffice') -> str:
        """
        Converts a DOCX file to PDF.
        Enforces fixed table layout first.
        Methods: 'libreoffice' (recommended), 'docx2pdf' (fallback).
        """
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Pre-process for table stability (Phase 4)
        cls._ensure_fixed_table_layout(input_path)

        output_dir = os.path.dirname(input_path)
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}.pdf")

        if method == 'libreoffice':
            return cls._convert_with_libreoffice(input_path, output_path)
        else:
            return cls._convert_with_docx2pdf(input_path, output_path)

    @classmethod
    def _convert_with_libreoffice(cls, input_path: str, output_path: str) -> str:
        """
        Uses LibreOffice headless with writer8 PDF filter.
        Best for preservation of structural tags and table alignment.
        """
        try:
            # Common paths on Windows for LibreOffice
            soffice_paths = [
                'soffice',
                r'C:\Program Files\LibreOffice\program\soffice.exe',
                r'C:\Program Files (x86)\LibreOffice\program\soffice.exe'
            ]
            
            soffice_bin = 'soffice'
            for p in soffice_paths:
                if os.path.exists(p) or p == 'soffice':
                    soffice_bin = p
                    break

            cmd = [
                soffice_bin,
                '--headless',
                '--convert-to', 'pdf:writer_pdf_Export', # writer8 filter
                '--outdir', os.path.dirname(output_path),
                input_path
            ]
            
            subprocess.run(cmd, check=True, capture_output=True)
            
            if os.path.exists(output_path):
                logger.info(f"LibreOffice conversion successful: {output_path}")
                return output_path
            raise Exception("LibreOffice output missing.")
            
        except Exception as e:
            logger.warning(f"LibreOffice failed, falling back: {e}")
            return cls._convert_with_docx2pdf(input_path, output_path)

    @classmethod
    def _convert_with_docx2pdf(cls, input_path: str, output_path: str) -> str:
        try:
            from docx2pdf import convert
            convert(input_path, output_path)
            if os.path.exists(output_path):
                return output_path
            raise Exception("docx2pdf output missing.")
        except Exception as e:
            error_msg = f"All PDF conversion methods failed: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
