import sys
import os

def check_env():
    print(f"Python: {sys.version}")
    print(f"CWD: {os.getcwd()}")
    
    try:
        import fitz
        print(f"PyMuPDF version: {fitz.version}")
    except ImportError:
        print("PyMuPDF (fitz) NOT FOUND")

    try:
        import pdfplumber
        print(f"pdfplumber version: {pdfplumber.__version__}")
    except ImportError:
        print("pdfplumber NOT FOUND")

    try:
        import pytesseract
        print("pytesseract library found. Checking binary...")
        try:
            from PIL import Image
            # Just check if version works
            ver = pytesseract.get_tesseract_version()
            print(f"Tesseract binary version: {ver}")
        except Exception as e:
            print(f"Tesseract binary NOT FOUND or failing: {e}")
    except ImportError:
        print("pytesseract library NOT FOUND")

    try:
        import cv2
        print(f"OpenCV version: {cv2.__version__}")
    except ImportError:
        print("OpenCV NOT FOUND")

if __name__ == "__main__":
    check_env()
