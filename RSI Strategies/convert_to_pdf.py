
from xhtml2pdf import pisa
import os

def convert_html_to_pdf(source_html, output_filename):
    with open(source_html, "r", encoding='utf-8') as f:
        html_content = f.read()
    
    with open(output_filename, "wb") as result_file:
        pisa_status = pisa.CreatePDF(html_content, dest=result_file)
        
    return pisa_status.err

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_html = os.path.join(script_dir, "RSI_Strategy_Report.html")
    output_pdf = os.path.join(script_dir, "RSI_Strategy_Report.pdf")
    
    print(f"Converting {source_html} to PDF...")
    
    if not os.path.exists(source_html):
        print(f"Error: HTML file not found at {source_html}")
    else:
        err = convert_html_to_pdf(source_html, output_pdf)
        
        if not err:
            print(f"Successfully created PDF at: {output_pdf}")
        else:
            print("Error creating PDF")
