"""Scale the rendered poster PDF to the Hack-A-Roo size: one page, 48 x 36 inches, landscape.

Usage: python docs/poster/resize_pdf.py in.pdf out.pdf
The poster is 4:3, the same shape as 48 x 36, so nothing is cropped or stretched. Text and
charts stay vector, so they stay sharp at poster size.
"""
import sys

from pypdf import PdfReader, PdfWriter

W, H = 48 * 72, 36 * 72  # points

reader = PdfReader(sys.argv[1])
assert len(reader.pages) == 1, "the poster must be one page"
writer = PdfWriter()
writer.add_page(reader.pages[0])
writer.pages[0].scale_to(W, H)
writer.write(sys.argv[2])
print(f"wrote {sys.argv[2]}: {W / 72:.0f} x {H / 72:.0f} in, 1 page")
